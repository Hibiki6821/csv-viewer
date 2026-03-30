from firebase_admin import credentials, firestore
import firebase_admin
import os

SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "fantia-csv-a7858d4bdd1b.json")

cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)
db = firestore.client()

src = db.collection("artifacts").document("fantia-analyzer-app").collection("accessdata").document("こおり").collection("Daily")
dst = db.collection("artifacts").document("fantia-analyzer-app").collection("accessdata").document("南ゆい").collection("Daily")

docs = src.get()
print(f"{len(docs)} 件コピーします...")

for doc in docs:
    dst.document(doc.id).set(doc.to_dict(), merge=True)
    print(f"  コピー完了: {doc.id}")

print("完了！")
