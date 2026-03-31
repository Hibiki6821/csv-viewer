"""
もりゆかの売上データをFirestoreから読み込んで異常を確認するデバッグスクリプト
"""
import hashlib
import os
from collections import defaultdict
import firebase_admin
from firebase_admin import credentials, firestore

SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "fantia-csv-a7858d4bdd1b.json")
cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)
db = firestore.client()

APP_ID = 'fantia-analyzer-app'
COMPANY_GROUP_NAME = 'ブルマ'
CAST_NAME = 'もりゆか'

company_group_id = hashlib.sha1(COMPANY_GROUP_NAME.encode()).hexdigest()
cast_id = hashlib.sha1(CAST_NAME.encode()).hexdigest()

print(f"companyGroupId: {company_group_id}")
print(f"castId:         {cast_id}")
print()

orders_ref = (
    db.collection('artifacts').document(APP_ID)
    .collection('public').document('data')
    .collection('companyGroups').document(company_group_id)
    .collection('casts').document(cast_id)
    .collection('orders')
)

docs = list(orders_ref.stream())
print(f"総注文ドキュメント数: {len(docs)}")
print()

# 各ドキュメントを検査
issues = []
by_month = defaultdict(list)
date_formats = defaultdict(int)

for doc in docs:
    d = doc.to_dict()
    doc_id = doc.id
    order_date = d.get('orderDate', '')
    price = d.get('price', 0)
    status = d.get('status', '')

    # 日付フォーマットチェック
    if '/' in order_date:
        date_formats['slash(旧フォーマット)'] += 1
    elif '-' in order_date:
        date_formats['hyphen(新フォーマット)'] += 1
    else:
        date_formats['unknown'] += 1
        issues.append(f"[不明な日付形式] docId={doc_id}, orderDate={order_date!r}")

    # ドキュメントIDと日付の整合性チェック
    date_prefix = order_date.replace('/', '-')[:10]
    if doc_id.startswith('20') and '_' in doc_id:
        id_date = doc_id[:10]
        if id_date != date_prefix:
            issues.append(f"[ID-日付不一致] docId={doc_id}, orderDate={order_date}")

    # 異常な金額チェック（負の値・極端に大きい値）
    if price < 0:
        issues.append(f"[負の金額] docId={doc_id}, price={price}, status={status}")
    if price > 1000000:
        issues.append(f"[異常に大きい金額] docId={doc_id}, price={price}, status={status}")

    # 月別集計
    month = order_date.replace('/', '-')[:7]
    if month:
        by_month[month].append({'price': price, 'status': status, 'docId': doc_id})

# 月別サマリー
print("=== 月別売上サマリー（取引完了のみ） ===")
for month in sorted(by_month.keys()):
    orders = by_month[month]
    completed = [o for o in orders if o['status'] == '取引完了']
    total = sum(o['price'] for o in completed)
    print(f"  {month}: {len(completed)}件, {total:,}円  (全ステータス: {len(orders)}件)")

print()
print("=== 日付フォーマット ===")
for fmt, count in date_formats.items():
    print(f"  {fmt}: {count}件")

# monthlyアーカイブも確認
print()
monthly_ref = (
    db.collection('artifacts').document(APP_ID)
    .collection('public').document('data')
    .collection('companyGroups').document(company_group_id)
    .collection('casts').document(cast_id)
    .collection('monthly')
)
monthly_docs = list(monthly_ref.stream())
print(f"=== monthlyアーカイブ ({len(monthly_docs)}チャンク) ===")
archive_by_month = defaultdict(int)
for mdoc in monthly_docs:
    orders_in_chunk = mdoc.to_dict().get('orders', [])
    month_key = mdoc.id[:7]
    archive_by_month[month_key] += len(orders_in_chunk)
    print(f"  {mdoc.id}: {len(orders_in_chunk)}件")

# アーカイブと個別ドキュメントの件数比較
print()
print("=== 個別doc vs アーカイブ 件数比較 ===")
all_months = sorted(set(list(by_month.keys()) + list(archive_by_month.keys())))
for month in all_months:
    individual = len(by_month.get(month, []))
    archived = archive_by_month.get(month, 0)
    match = "✓" if individual == archived else "❌ 不一致!"
    print(f"  {month}: 個別={individual}件, アーカイブ={archived}件 {match}")

# 問題一覧
print()
if issues:
    print(f"=== 問題点 ({len(issues)}件) ===")
    for issue in issues:
        print(f"  {issue}")
else:
    print("=== 問題点: なし ===")
