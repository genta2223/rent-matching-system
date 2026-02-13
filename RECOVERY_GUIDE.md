# 復旧・トラブルシューティングガイド (RECOVERY_GUIDE.md)

このドキュメントは、エージェントが中断（トークン上限など）から復帰した際、またはトラブルが発生した際のガイドです。

## 1. エージェントの復帰手順
もし作業が中断した場合、以下の順序で状況を確認してください。

1.  **環境変数の確認**: `.env` ファイルに `SUPABASE_URL` と `SUPABASE_KEY` が存在するか確認する。
2.  **最新のタスク確認**: `task.md` および `walkthrough.md` を読み、どこまで完了しているか把握する。
3.  **DB接続確認**: `db_client.py` を使用して、Supabaseからデータが取得できるかテストする。
4.  **プロセスの再開**: `streamlit run app.py` でアプリを起動し、UI上で動作を確認する。

## 2. よくある問題と解決策

### Q. 「延滞者のみ」の判定件数が異常に多い（12件など）
- **原因1**: `payment_ledger.csv` が最新ではない（手元の最新CSVをインポートしていない）。
- **原因2**: 備考欄（`LatestPaymentMemo`）に `None` または `nan` という文字列が入っており、正規の解析フローがバイパスされている。
- **解決策**: `matcher_db.py` の `TenantRecordDB` クラスにおける `None` 処理を確認。またはアプリの「1. 銀行データ取込」から最新データを入れる。

### Q. 請求書ZIPに特定の物件が含まれない（Prop 11など）
- **原因**: 該当物件の `SeparateAccountManagement` が `True` (1.0) になっている。
- **解決策**: `rent_roll.csv` またはアプリの編集画面でフラグを確認してください。

### Q. PDFの日本語が文字化けする / 豆腐になる
- **原因**: フォントファイル（`msgothic.ttc`）がシステムの `C:\Windows\Fonts` に存在しない、または ReportLab が読み込めていない。
- **解決策**: `invoice_generator_web.py` のフォントパス設定を確認。標準フォント（Helvetica）にフォールバックされている場合は、日本語は表示されません。

## 3. GitHubへの同期
個人情報を除いたコードベースをGitHubにプッシュするには以下のコマンドを使用します。

```powershell
git add .
git commit -m "Update technical specs and core logic"
git push origin main
```
※ `private_data/` フォルダは `.gitignore` により除外されていることを常に確認してください。
