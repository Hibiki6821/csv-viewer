"""
GA4アクセスデータ（accessdata）を削除するスクリプト。

使い方:
  python delete_accessdata.py
    - 全サイト/全期間の削除対象を確認（DRY RUN）

  python delete_accessdata.py --apply
    - 全サイト/全期間を実際に削除

  python delete_accessdata.py --site まりの --from 2026-03-01 --to 2026-03-10 --apply
    - 指定サイトの指定期間だけ削除
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import InvalidArgument

APP_ID = "fantia-analyzer-app"
DATE_ID_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DELETE_BATCH_SIZE = 40
KNOWN_SITE_NAMES = ["まりの", "南ゆい", "もりゆか", "かなめりあ", "夜猫みるく", "あい"]


def validate_date_arg(value: str) -> str:
    if not DATE_ID_RE.match(value):
        raise argparse.ArgumentTypeError(f"日付形式が不正です: {value} (YYYY-MM-DD)")
    return value


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Firestoreのaccessdata削除")
    p.add_argument("--apply", action="store_true", help="実際に削除する（未指定時はDRY RUN）")
    p.add_argument("--site", action="append", help="対象サイト名（複数指定可）")
    p.add_argument("--from", dest="date_from", type=validate_date_arg, help="開始日 YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", type=validate_date_arg, help="終了日 YYYY-MM-DD")
    return p.parse_args()


def init_firestore():
    sa_path = Path(__file__).parent / "fantia-csv-a7858d4bdd1b.json"
    cred = credentials.Certificate(str(sa_path))
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()


def list_target_sites(db, explicit_sites: list[str] | None) -> list[str]:
    if explicit_sites:
        return explicit_sites
    sites_ref = (
        db.collection("artifacts")
        .document(APP_ID)
        .collection("accessdata")
    )
    detected = [d.id for d in sites_ref.stream()]
    if detected:
        return detected
    # 親ドキュメントにフィールドが無い場合、collection一覧に現れないため既知サイトへフォールバック
    print("accessdata直下のサイトドキュメントが見つからないため、既知サイト名を対象にします。")
    return KNOWN_SITE_NAMES


def in_range(date_id: str, date_from: str | None, date_to: str | None) -> bool:
    if date_from and date_id < date_from:
        return False
    if date_to and date_id > date_to:
        return False
    return True


def commit_delete_refs(db, refs):
    batch = db.batch()
    for ref in refs:
        batch.delete(ref)
    batch.commit()


def delete_refs_with_split_retry(db, refs):
    """Transaction too big が出たら自動で分割して再試行する。"""
    if not refs:
        return
    try:
        commit_delete_refs(db, refs)
    except InvalidArgument as e:
        msg = str(e)
        if "Transaction too big" not in msg or len(refs) == 1:
            raise
        mid = len(refs) // 2
        delete_refs_with_split_retry(db, refs[:mid])
        delete_refs_with_split_retry(db, refs[mid:])


def main():
    args = parse_args()
    dry_run = not args.apply
    mode = "DRY RUN" if dry_run else "本番実行"
    print(f"=== accessdata削除スクリプト [{mode}] ===")

    if args.date_from and args.date_to and args.date_from > args.date_to:
        raise SystemExit(f"--from ({args.date_from}) は --to ({args.date_to}) 以下にしてください。")

    db = init_firestore()
    sites = list_target_sites(db, args.site)
    if not sites:
        print("対象サイトが見つかりません。")
        return

    total_docs = 0
    for site_name in sites:
        daily_ref = (
            db.collection("artifacts")
            .document(APP_ID)
            .collection("accessdata")
            .document(site_name)
            .collection("Daily")
        )
        docs = list(daily_ref.stream())
        target_docs = [
            d for d in docs
            if DATE_ID_RE.match(d.id) and in_range(d.id, args.date_from, args.date_to)
        ]

        if not target_docs:
            print(f"{site_name}: 対象データなし")
            continue

        print(f"{site_name}: {len(target_docs)}件 {'削除予定' if dry_run else '削除'}")
        total_docs += len(target_docs)

        if dry_run:
            sample = ", ".join(d.id for d in target_docs[:10])
            if sample:
                print(f"  例: {sample}{' ...' if len(target_docs) > 10 else ''}")
            continue

        refs = [d.reference for d in target_docs]
        for i in range(0, len(refs), DELETE_BATCH_SIZE):
            chunk = refs[i:i + DELETE_BATCH_SIZE]
            delete_refs_with_split_retry(db, chunk)

    print(f"\n合計: {total_docs}件 {'削除予定' if dry_run else '削除完了'}")
    if dry_run:
        print("実際に削除するには --apply を付けて再実行してください。")


if __name__ == "__main__":
    main()
