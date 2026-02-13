# 家賃管理システム 技術仕様書 (TECHNICAL_SPEC.md)

このドキュメントは、家賃管理システムのアーキテクチャ、核心ロジック、および仕様をまとめたものです。

## 1. システム概要
本システムは、銀行振込データと入居者台帳を照合し、延滞状況の管理および請求書の自動生成を行うWebアプリケーションです。

- **Frontend**: Streamlit
- **Backend**: Supabase (PostgreSQL)
- **PDF Generation**: ReportLab
- **Data Processing**: Pandas

## 2. データ構造 (Supabase Schema)

### tenants テーブル
- `PropertyID` (TEXT, PK): 物件番号
- `TenantName` (TEXT): 入居者名
- `MonthlyRent` (NUMERIC): 月額家賃
- `BaseDebtAmount` (NUMERIC): 基準日時点の残高（前月繰越分）
- `BaseDebtDate` (DATE): 基準日（この月より後の家賃が加算される）
- `LatestPaymentMemo` (TEXT): 備考欄（支払状況のテキスト解析用）
- `SeparateAccountManagement` (BOOLEAN): 口座別管理フラグ（Trueの場合、一括請求から除外）

### payments テーブル
- `TransactionKey` (TEXT, UNIQUE): 振込の一意なキー
- `PropertyID` (TEXT): 物件番号
- `PaymentDate` (DATE): 入金日
- `Amount` (NUMERIC): 入金額

## 3. 核心ロジック (matcher_db.py)

### 3.1 債務計算 (`calculate_debts`)
1. **基準日 Snapshot**: `BaseDebtDate` がある場合、その月の残高として `BaseDebtAmount` をセットし、翌月から現在までの月額家賃を債務として積み上げます。
2. **備考欄解析 (Fallback)**: 基準日がない場合、`LatestPaymentMemo` を解析します。「〇月分全額」などの記述から、過去の未払月を特定します。
3. **初期化**: いずれもない場合は、数ヶ月前からの家賃を便宜上の債務として開始します。

### 3.2 入金充当 (`allocate_payments`)
- **FIFO (先入先出)**: 最も古い月の債務から順に入金額を充当します。
- **過剰金処理**: 債務を上回る入金は「余剰金」として保持し、将来の債務に充てます。

### 3.3 請求判定
- **延滞の定義**: `Balance > 10 JPY` （端数処理の影響を排除するため10円を閾値としています）。

## 4. 特筆すべき仕様・ハマりどころ
- **None値の適正化**: Supabaseから取得したデータが `None` の場合、文字列の `"None"` や `"nan"` に変換されるのを防ぎ、空文字として扱う必要があります（`matcher_db.py`の`TenantRecordDB`クラスで実装済み）。
- **物件IDの型**: CSVから読み込む際、PropertyIDが `float` (11.0) になることがあり、文字列の `"11"` と不一致を起こします。常に `.0` を除去する正規化が必要です。
- **管理対象外物件**: `SeparateAccountManagement` が `True` の物件は、延滞があっても一括請求の対象から除外されます。

## 5. 請求書生成 (Invoicing)
- **Web対応**: メモリ上の `BytesIO` を使用してPDFを生成し、ZIP形式でまとめてダウンロード可能にしています。
- **窓付き封筒対応**: 宛先（氏名・住所）を左上に配置するレイアウトを採用しています。
