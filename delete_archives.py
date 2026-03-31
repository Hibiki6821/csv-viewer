"""
全キャストの monthly アーカイブを削除するスクリプト。
migrate_doc_ids.py --apply で旧フォーマット変換後、このスクリプトを実行してください。
次回アクセス時にアーカイブが正しく再作成されます。

使い方:
  python delete_archives.py           # 削除対象を確認のみ（ドライラン）
  python delete_archives.py --apply   # 実際に削除
"""
import hashlib
import json
import sys
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore

DRY_RUN = '--apply' not in sys.argv

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

mode = 'DRY RUN' if DRY_RUN else '本番実行'
print(f'=== monthlyアーカイブ削除スクリプト [{mode}] ===\n')

total = 0
for cast in config['casts']:
    cast_name = cast['cast_name']
    cast_id = hashlib.sha1(cast_name.encode()).hexdigest()

    monthly_ref = (
        db.collection('artifacts').document(APP_ID)
        .collection('public').document('data')
        .collection('companyGroups').document(company_group_id)
        .collection('casts').document(cast_id)
        .collection('monthly')
    )
    docs = list(monthly_ref.stream())
    if not docs:
        print(f'{cast_name}: アーカイブなし（スキップ）')
        continue

    print(f'{cast_name}: {len(docs)} チャンク削除{"予定" if DRY_RUN else ""}')
    for doc in docs:
        orders_count = len(doc.to_dict().get('orders', []))
        print(f'  {doc.id}: {orders_count}件')
        if not DRY_RUN:
            doc.reference.delete()
    total += len(docs)

print(f'\n合計: {total} チャンク{"削除予定" if DRY_RUN else "削除完了"}')
if DRY_RUN:
    print('\n実際に削除するには: python delete_archives.py --apply')
