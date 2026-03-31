"""
全キャストの monthly アーカイブを Firestore に作成するスクリプト。
ブラウザからの初回アクセスを高速化するために事前に実行してください。

使い方:
  python build_archives.py           # 全キャストのアーカイブを作成
  python build_archives.py もりゆか  # 特定キャストのみ
"""
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore

CHUNK_SIZE = 800
# Firestore Commit request が大きくなりすぎないようにするための上限
# 実際の上限(10MiB)よりかなり低めに設定して安全側で分割する。
MAX_BATCH_BYTES = 7_000_000
MAX_BATCH_OPS = 60
MAX_DELETE_OPS = 20

config_path = Path(__file__).parent / 'fantia_uploader' / 'config.json'
with open(config_path, encoding='utf-8') as f:
    config = json.load(f)

sa_path = Path(__file__).parent / 'fantia-csv-a7858d4bdd1b.json'
cred = credentials.Certificate(str(sa_path))
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)
db = firestore.client()

APP_ID = 'fantia-analyzer-app'
company_group_id = hashlib.sha1(config['company_group_name'].encode()).hexdigest()

# 対象キャストを絞り込む（引数で指定可能）
target_name = sys.argv[1] if len(sys.argv) > 1 else None
casts = [c for c in config['casts'] if target_name is None or c['cast_name'] == target_name]

if not casts:
    print(f'キャスト "{target_name}" が config.json に見つかりません。')
    sys.exit(1)

print(f'=== monthly アーカイブ作成スクリプト ===')
print(f'対象: {", ".join(c["cast_name"] for c in casts)}\n')


def build_archive_for_cast(cast_name: str):
    cast_id = hashlib.sha1(cast_name.encode()).hexdigest()
    orders_ref = (
        db.collection('artifacts').document(APP_ID)
        .collection('public').document('data')
        .collection('companyGroups').document(company_group_id)
        .collection('casts').document(cast_id)
        .collection('orders')
    )
    monthly_ref = (
        db.collection('artifacts').document(APP_ID)
        .collection('public').document('data')
        .collection('companyGroups').document(company_group_id)
        .collection('casts').document(cast_id)
        .collection('monthly')
    )

    # 既存アーカイブを削除
    existing = list(monthly_ref.stream())
    if existing:
        print(f'  既存アーカイブ {len(existing)} チャンクを削除中...')
        batch = db.batch()
        delete_ops = 0
        for doc in existing:
            batch.delete(doc.reference)
            delete_ops += 1
            if delete_ops >= MAX_DELETE_OPS:
                batch.commit()
                batch = db.batch()
                delete_ops = 0
        if delete_ops > 0:
            batch.commit()

    # 全注文を読み込み
    print(f'  注文データ読み込み中...')
    docs = list(orders_ref.stream())
    print(f'  {len(docs)} 件取得')

    # 月別に振り分け（orderDate を正規化して月を取得）
    by_month = defaultdict(list)
    skipped = 0
    for doc in docs:
        data = doc.to_dict()
        order_date = data.get('orderDate', '').replace('/', '-')
        month = order_date[:7]  # YYYY-MM
        if not month or len(month) != 7:
            skipped += 1
            continue
        # orderDate を正規化して保存
        data['orderDate'] = order_date
        by_month[month].append(data)

    if skipped:
        print(f'  日付不正でスキップ: {skipped} 件')

    # 月次アーカイブを書き込み（チャンク分割）
    # 注意: 1回のcommitに大きなドキュメントを詰め込みすぎると
    # "Transaction too big. Decrease transaction size." が発生するため、
    # 件数と概算バイト数の両方で分割する。
    ts = __import__('datetime').datetime.utcnow().isoformat() + 'Z'
    total_chunks = 0
    batch = db.batch()
    batch_ops = 0
    batch_bytes = 0

    def flush_batch():
        nonlocal batch, batch_ops, batch_bytes
        if batch_ops == 0:
            return
        batch.commit()
        batch = db.batch()
        batch_ops = 0
        batch_bytes = 0

    for month in sorted(by_month.keys()):
        orders = by_month[month]
        chunks = [orders[i:i + CHUNK_SIZE] for i in range(0, len(orders), CHUNK_SIZE)]
        for i, chunk in enumerate(chunks):
            chunk_id = month if i == 0 else f'{month}_{i}'
            doc_data = {'orders': chunk, 'lastUpdated': ts}
            # json化した概算サイズでcommit分割
            estimated_bytes = len(json.dumps(doc_data, ensure_ascii=False).encode('utf-8'))

            if (
                batch_ops >= MAX_BATCH_OPS
                or (batch_ops > 0 and batch_bytes + estimated_bytes > MAX_BATCH_BYTES)
            ):
                flush_batch()

            batch.set(monthly_ref.document(chunk_id), doc_data)
            batch_ops += 1
            batch_bytes += estimated_bytes

        total_chunks += len(chunks)
        chunk_info = f'{len(chunks)} チャンク' if len(chunks) > 1 else ''
        print(f'  {month}: {len(orders)} 件 {chunk_info}')

    flush_batch()

    print(f'  → 合計 {sum(len(v) for v in by_month.values())} 件, {total_chunks} チャンク書き込み完了\n')


def build_daily_summary():
    """全キャストの注文データを日別に集計して daily_summary コレクションに書き込む"""
    print('=== daily_summary 作成 ===')

    daily_summary_ref = (
        db.collection('artifacts').document(APP_ID)
        .collection('public').document('data')
        .collection('companyGroups').document(company_group_id)
        .collection('daily_summary')
    )

    daily_data = defaultdict(lambda: {'casts': {}})

    # daily_summary は常に全キャスト対象（CLI でキャストを絞っても全員分を再構築する）
    all_casts = config['casts']
    for cast in all_casts:
        cast_name = cast['cast_name']
        cast_id = hashlib.sha1(cast_name.encode()).hexdigest()
        orders_ref = (
            db.collection('artifacts').document(APP_ID)
            .collection('public').document('data')
            .collection('companyGroups').document(company_group_id)
            .collection('casts').document(cast_id)
            .collection('orders')
        )
        docs = list(orders_ref.stream())
        print(f'  {cast_name}: {len(docs)}件')

        by_date = defaultdict(lambda: {'revenue': 0, 'orders': 0})
        for doc in docs:
            data = doc.to_dict()
            if data.get('status') != '取引完了':
                continue
            order_date = data.get('orderDate', '').replace('/', '-')[:10]
            if not order_date or len(order_date) != 10:
                continue
            by_date[order_date]['revenue'] += data.get('price', 0)
            by_date[order_date]['orders'] += 1

        for date_str, stats in by_date.items():
            daily_data[date_str]['casts'][cast_id] = {
                'castName': cast_name,
                'revenue': stats['revenue'],
                'orders': stats['orders'],
            }

    existing = list(daily_summary_ref.stream())
    if existing:
        print(f'  既存 {len(existing)} 件を削除中...')
        batch = db.batch()
        delete_ops = 0
        for doc in existing:
            batch.delete(doc.reference)
            delete_ops += 1
            if delete_ops >= MAX_DELETE_OPS:
                batch.commit()
                batch = db.batch()
                delete_ops = 0
        if delete_ops > 0:
            batch.commit()

    ts = __import__('datetime').datetime.utcnow().isoformat() + 'Z'
    batch = db.batch()
    batch_count = 0
    for date_str in sorted(daily_data.keys()):
        doc_data = dict(daily_data[date_str])
        doc_data['lastUpdated'] = ts
        batch.set(daily_summary_ref.document(date_str), doc_data)
        batch_count += 1
        if batch_count >= 498:
            batch.commit()
            batch = db.batch()
            batch_count = 0
    if batch_count > 0:
        batch.commit()

    print(f'  → {len(daily_data)} 日分の daily_summary を書き込みました\n')


for cast in casts:
    print(f'--- {cast["cast_name"]} ---')
    build_archive_for_cast(cast['cast_name'])

build_daily_summary()

print('完了！')
