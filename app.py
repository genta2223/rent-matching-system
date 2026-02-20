import streamlit as st
import pandas as pd
import re
from db_client import DBClient
from datetime import datetime
from matcher_db import LogicEngine

# Page Config
st.set_page_config(page_title="家賃管理システム", layout="wide")

st.title("家賃管理システム (クラウド版)")

# Initialize DB Client
@st.cache_resource
def get_db_client():
    return DBClient()

try:
    db = get_db_client()
    st.success("データベース接続成功")
except Exception as e:
    st.error(f"データベース接続エラー: {e}")
    st.stop()

# Auto-refresh logic
if st.button("表示データ更新"):
    st.cache_data.clear()

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["入居者台帳 (Rent Roll)", "入金明細 (Ledger)", "管理ツール"])

with tab1:
    st.subheader("入居者一覧")
    try:
        tenants_df = db.fetch_tenants()
        
        if not tenants_df.empty:
            # Display Key Metrics
            total_rent = tenants_df['MonthlyRent'].sum()
            st.metric("月額家賃総額（想定）", f"¥{total_rent:,}")
            
            # Normalize Values column
            tenants_df['Values'] = tenants_df['Values'].apply(lambda x: x if isinstance(x, dict) else {})
            
            # Extract common nested fields
            tenants_df['BankMatchName1'] = tenants_df['Values'].apply(lambda x: x.get('BankMatchName1', ''))
            tenants_df['BankMatchName2'] = tenants_df['Values'].apply(lambda x: x.get('BankMatchName2', ''))
            tenants_df['BankMatchName3'] = tenants_df['Values'].apply(lambda x: x.get('BankMatchName3', ''))
            tenants_df['Agent'] = tenants_df['Values'].apply(lambda x: x.get('Agent', ''))
            tenants_df['Manager'] = tenants_df['Values'].apply(lambda x: x.get('Manager', ''))
            tenants_df['SeparateAccountManagement'] = tenants_df['Values'].apply(lambda x: x.get('SeparateAccountManagement', '0'))
            
            # New fields for fixed base date logic
            tenants_df['base_date'] = tenants_df['Values'].apply(lambda x: x.get('base_date', '2026-02-13'))
            tenants_df['base_debt'] = tenants_df['Values'].apply(lambda x: float(x.get('base_debt', 0)))
            tenants_df['base_surplus'] = tenants_df['Values'].apply(lambda x: float(x.get('base_surplus', 0)))
            tenants_df['manual_adjustment'] = tenants_df['Values'].apply(lambda x: float(x.get('manual_adjustment', 0)))
            tenants_df['adjustment_memo'] = tenants_df['Values'].apply(lambda x: x.get('adjustment_memo', ''))
            
            # Auto absorb fields
            tenants_df['auto_absorb_enabled'] = tenants_df['Values'].apply(lambda x: bool(x.get('auto_absorb_enabled', False)))
            tenants_df['auto_absorb_limit'] = tenants_df['Values'].apply(lambda x: float(x.get('auto_absorb_limit', 0.0)))
            tenants_df['auto_absorb_label'] = tenants_df['Values'].apply(lambda x: x.get('auto_absorb_label', ''))
            
            # Additional flags
            def get_clean_start(row):
                vals = row.get('Values', {})
                if 'is_clean_start' in vals:
                    raw = vals['is_clean_start']
                    if isinstance(raw, str): return raw.lower() in ('true', '1', 't', 'y', 'yes')
                    return bool(raw)
                try:
                    debt = float(vals.get('base_debt', row.get('BaseDebtAmount', 0)))
                except:
                    debt = 0.0
                return debt <= 0

            tenants_df['is_clean_start'] = tenants_df.apply(get_clean_start, axis=1)
            tenants_df['last_confirmed_date'] = tenants_df['Values'].apply(lambda x: x.get('last_confirmed_date', ''))
            
            # Reorder columns
            cols = [
                'PropertyID', 'Name', 'MonthlyRent', 
                'is_clean_start', 'last_confirmed_date', 
                'base_date', 'base_debt', 'base_surplus', 'manual_adjustment', 'adjustment_memo',
                'auto_absorb_enabled', 'auto_absorb_limit', 'auto_absorb_label',
                'BankMatchName1', 'BankMatchName2', 'BankMatchName3', 
                'Memo', 'Agent', 'Manager', 'SeparateAccountManagement'
            ]
            other_cols = [c for c in tenants_df.columns if c not in cols and c != 'Values']
            tenants_df = tenants_df[cols + other_cols]

            edited_df = st.data_editor(
                tenants_df, 
                use_container_width=True, 
                num_rows="dynamic",
                key="tenant_editor"
            )
            
            if st.button("入居者情報の変更を保存"):
                try:
                    records = []
                    for _, row in edited_df.iterrows():
                        record = row.to_dict()
                        values = {
                            'BankMatchName1': record.pop('BankMatchName1', None),
                            'BankMatchName2': record.pop('BankMatchName2', None),
                            'BankMatchName3': record.pop('BankMatchName3', None),
                            'Agent': record.pop('Agent', None),
                            'Manager': record.pop('Manager', None),
                            'SeparateAccountManagement': record.pop('SeparateAccountManagement', '0'),
                            'base_date': record.pop('base_date', '2026-02-13'),
                            'base_debt': record.pop('base_debt', 0),
                            'base_surplus': record.pop('base_surplus', 0),
                            'manual_adjustment': record.pop('manual_adjustment', 0),
                            'adjustment_memo': record.pop('adjustment_memo', ''),
                            'auto_absorb_enabled': record.pop('auto_absorb_enabled', False),
                            'auto_absorb_limit': record.pop('auto_absorb_limit', 0.0),
                            'auto_absorb_label': record.pop('auto_absorb_label', ''),
                            'is_clean_start': record.pop('is_clean_start', False),
                            'last_confirmed_date': record.pop('last_confirmed_date', '')
                        }
                        record['Values'] = values
                        records.append(record)
                    
                    db.upsert_tenants(records)
                    st.success("入居者データを更新しました！")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"保存エラー: {e}")
            
            st.markdown("---")
            st.write("Excelで編集する場合は、以下のボタンから最新版をダウンロードしてください（文字化け防止用）。")
            csv_data = tenants_df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="レントロールをExcel用(CSV)でダウンロード",
                data=csv_data,
                file_name=f"rent_roll_export_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info("入居者データがありません。")
    except Exception as e:
        st.error(f"読み込みエラー: {e}")

with tab2:
    st.subheader("入金履歴")
    try:
        payments_df = db.fetch_payments()
        if not payments_df.empty:
            st.dataframe(payments_df.sort_values(by="Date", ascending=False), use_container_width=True)
        else:
            st.info("入金データが見つかりません。")
    except Exception as e:
        st.error(f"読み込みエラー: {e}")

with tab3:
    st.subheader("一括管理・データ更新")
    
    # Section 1: Bank Data Import with AI-like auto detection
    st.markdown("### 1. 銀行データ取込 (自動判定)")
    st.write("銀行からダウンロードしたCSVをそのままアップロードしてください。")
    uploaded_file = st.file_uploader("銀行CSVファイルをアップロード", type=["csv"])
    
    if uploaded_file is not None:
        try:
            # --- Step 1: Read CSV with encoding detection ---
            try:
                import chardet
                raw_data = uploaded_file.read()
                result = chardet.detect(raw_data)
                encoding = result['encoding'] if result['encoding'] else 'cp932'
                uploaded_file.seek(0)
                bank_df = pd.read_csv(uploaded_file, encoding=encoding)
            except Exception:
                uploaded_file.seek(0)
                bank_df = pd.read_csv(uploaded_file, encoding='cp932')
            
            st.write(f"📊 読み込み: **{len(bank_df)} 行** × {len(bank_df.columns)} 列")
            
            # --- Step 2: Template lookup or heuristic detection ---
            from csv_ai_mapper import HeuristicMapper, TemplateManager
            
            cols = bank_df.columns.tolist()
            header_hash = TemplateManager.get_header_hash(cols)
            saved_template = TemplateManager.lookup(db, cols)
            
            if saved_template:
                # Known template — skip confirmation
                mapping = saved_template['mapping']
                label = saved_template.get('label', '不明')
                st.success(f"✅ 登録済みテンプレート「{label}」を適用します (ハッシュ: {header_hash[:8]})")
                needs_confirmation = False
            else:
                # New layout — run heuristic detection
                mapping = HeuristicMapper.suggest_mapping(bank_df)
                st.warning(f"🔍 **新しいCSVレイアウトを検知しました** (ハッシュ: {header_hash[:8]})")
                needs_confirmation = True
            
            # --- Step 3: Show mapping preview & allow manual override ---
            if needs_confirmation:
                st.markdown("#### 推定結果の確認")
                
                conf_pct = int(mapping.get('confidence', 0) * 100)
                st.info(f"推定精度: **{conf_pct}%**")
                
                col1, col2 = st.columns(2)
                with col1:
                    # Date display
                    if mapping.get('date_parts'):
                        parts = mapping['date_parts']
                        st.write(f"📅 日付: `{parts['year']}` / `{parts['month']}` / `{parts['day']}` (年/月/日)")
                    elif mapping.get('date'):
                        st.write(f"📅 日付: `{mapping['date']}`")
                    else:
                        st.error("⚠️ 日付列が見つかりません")
                    
                    st.write(f"💰 金額: `{mapping.get('amount', '未検出')}`")
                    st.write(f"👤 振込人: `{mapping.get('sender', '未検出')}`")
                    if mapping.get('deposit_filter'):
                        st.write(f"🏷 入出金フィルター: `{mapping['deposit_filter']}`")
                
                with col2:
                    st.write("**CSVプレビュー (先頭3行)**")
                    st.dataframe(bank_df.head(3), use_container_width=True)
                
                # Manual override
                with st.expander("🔧 手動で修正する場合はこちら"):
                    date_mode = st.radio("日付形式", ["年/月/日 個別列", "単一列"], 
                                         index=0 if mapping.get('date_parts') else 1)
                    
                    if date_mode == "年/月/日 個別列":
                        dp = mapping.get('date_parts') or {}
                        y_col = st.selectbox("年の列", cols, index=cols.index(dp.get('year', cols[0])) if dp.get('year') in cols else 0)
                        m_col = st.selectbox("月の列", cols, index=cols.index(dp.get('month', cols[0])) if dp.get('month') in cols else 0)
                        d_col = st.selectbox("日の列", cols, index=cols.index(dp.get('day', cols[0])) if dp.get('day') in cols else 0)
                        mapping['date_parts'] = {'year': y_col, 'month': m_col, 'day': d_col}
                        mapping['date'] = None
                    else:
                        d_col = st.selectbox("日付の列", cols, index=cols.index(mapping.get('date', cols[0])) if mapping.get('date') in cols else 0)
                        mapping['date'] = d_col
                        mapping['date_parts'] = None
                    
                    mapping['amount'] = st.selectbox("金額の列", cols, index=cols.index(mapping.get('amount', cols[0])) if mapping.get('amount') in cols else 0)
                    mapping['sender'] = st.selectbox("振込人/摘要の列", cols, index=cols.index(mapping.get('sender', cols[0])) if mapping.get('sender') in cols else 0)
                
                # Template label and confirm
                template_label = st.text_input("テンプレート名（例: りそな銀行）", value="")
                
                if st.button("✅ このマッピングで確定・保存"):
                    TemplateManager.save_template(db, cols, mapping, label=template_label)
                    st.success(f"テンプレート「{template_label}」を保存しました！次回から自動適用されます。")
                    needs_confirmation = False
                    st.rerun()
            
            # --- Step 4: Normalize and preview matched data ---
            if not needs_confirmation:
                try:
                    normalized = HeuristicMapper.normalize_bank_data(bank_df, mapping)
                    st.write(f"🔄 入金データ抽出: **{len(normalized)} 件**")
                    
                    if len(normalized) == 0:
                        st.warning("入金データが見つかりませんでした。マッピングを確認してください。")
                    else:
                        # Match to tenants
                        tenants_df = db.fetch_tenants()
                        payments_df = db.fetch_payments()
                        engine = LogicEngine(tenants_df, payments_df)
                        
                        # Build mapping in old format for match_new_bank_data compatibility
                        old_mapping = {
                            'date': ['Date'],
                            'amount': 'Amount',
                            'sender': 'Summary',
                            'type': None
                        }
                        new_entries = engine.match_new_bank_data(normalized, mapping=old_mapping)
                        
                        if new_entries:
                            st.info(f"🎯 テナントマッチング: **{len(new_entries)} 件**")
                            preview_df = pd.DataFrame(new_entries)
                            st.dataframe(preview_df, use_container_width=True)
                            
                            if st.button("📥 確定して入金データを登録"):
                                try:
                                    db.upsert_payments(new_entries)
                                    st.success(f"✅ {len(new_entries)} 件の入金データを登録しました！")
                                    st.cache_data.clear()
                                except Exception as e:
                                    st.error(f"登録エラー: {e}")
                        else:
                            st.warning("新規の入金データは見つかりませんでした（既に登録済みか、マッチするテナントがありません）。")
                except Exception as e:
                    st.error(f"データ正規化エラー: {e}")
                    
                # Option to reset template
                if st.button("🗑 テンプレートをリセット"):
                    TemplateManager.delete_template(db, cols)
                    st.info("テンプレートを削除しました。次回アップロード時に再確認されます。")
                    st.rerun()

        except Exception as e:
            st.error(f"ファイル処理エラー: {e}")


    st.markdown("---")
    
    # Section 2: Rent Roll Bulk Update
    st.markdown("### 2. 入居者台帳一括更新 (Rent Roll CSV)")
    rent_file = st.file_uploader("rent_roll.csv をアップロード", type=["csv"])
    
    if rent_file is not None:
        try:
            try:
                rent_df = pd.read_csv(rent_file, encoding='cp932')
            except UnicodeDecodeError:
                rent_file.seek(0)
                rent_df = pd.read_csv(rent_file, encoding='utf-8')
            
            # Auto-correction for common CSV issues
            fixed_count = 0
            for i, row in rent_df.iterrows():
                b_amt = row.get('BaseDebtAmount')
                b_date = row.get('BaseDebtDate')
                if isinstance(b_amt, str) and re.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$', b_amt):
                    if pd.isna(b_date) or b_date == '':
                        rent_df.at[i, 'BaseDebtDate'] = b_amt
                        rent_df.at[i, 'BaseDebtAmount'] = 0
                        fixed_count += 1
            
            if fixed_count > 0:
                st.warning(f"{fixed_count} 件のデータ列ズレを自動修正しました。")
            
            st.write(f"読み込み: {len(rent_df)} 件")
            st.dataframe(rent_df.head())
            
            if st.button("入居者台帳を一括更新"):
                try:
                    records = []
                    for _, row in rent_df.iterrows():
                        try:
                            raw_rent = row.get('MonthlyRent')
                            rent = int(raw_rent) if pd.notna(raw_rent) else 0
                        except:
                            rent = 0
                        
                        try:
                            raw_debt = row.get('BaseDebtAmount', 0)
                            base_debt = float(raw_debt) if pd.notna(raw_debt) and str(raw_debt).strip() != '' else 0.0
                        except:
                            base_debt = 0.0
                        
                        record = {
                            "PropertyID": str(row['PropertyID']),
                            "Name": row['TenantName'],
                            "MonthlyRent": rent,
                            "BaseDebtAmount": base_debt,
                            "BaseDebtDate": str(row.get('BaseDebtDate')) if pd.notna(row.get('BaseDebtDate')) else None,
                            "Zip": row.get('Zip'),
                            "Address": row.get('Address'),
                            "Tel": row.get('Tel'),
                            "Memo": row.get('Memo'),
                            "LatestPaymentMemo": row.get('LatestPaymentMemo'),
                            "Values": {
                                "Agent": row.get('Agent'),
                                "Manager": row.get('Manager'),
                                "BankMatchName1": row.get('BankMatchName1'),
                                "BankMatchName2": row.get('BankMatchName2'),
                                "BankMatchName3": row.get('BankMatchName3'),
                                "SeparateAccountManagement": row.get('SeparateAccountManagement')
                            }
                        }
                        records.append(record)
                    
                    db.upsert_tenants(records)
                    st.success(f"{len(records)} 件のデータを更新しました。")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"更新エラー: {e}")
        except Exception as e:
            st.error(f"ファイル読み込みエラー: {e}")

    st.markdown("---")
    
    # Section 3: Payment Ledger Bulk Update
    st.markdown("### 3. 入金台帳一括更新 (Payment Ledger CSV)")
    ledger_file = st.file_uploader("payment_ledger.csv をアップロード", type=["csv"])
    
    if ledger_file is not None:
        try:
            try:
                ledger_df = pd.read_csv(ledger_file, encoding='cp932')
            except UnicodeDecodeError:
                ledger_file.seek(0)
                ledger_df = pd.read_csv(ledger_file, encoding='utf-8')
            
            st.write(f"読み込み: {len(ledger_df)} 件")
            st.dataframe(ledger_df.head())
            
            if st.button("入金台帳を一括更新"):
                try:
                    records = []
                    for _, row in ledger_df.iterrows():
                        pid = str(row['PropertyID'])
                        if pid.endswith('.0'): pid = pid[:-2]
                        
                        record = {
                            "PropertyID": pid,
                            "Date": row['PaymentDate'],
                            "Amount": float(row['Amount']),
                            "Summary": row.get('Summary', ''),
                            "TransactionKey": row.get('TransactionKey'),
                            "AllocationDesc": row.get('AllocationDesc', '')
                        }
                        
                        if not record['TransactionKey'] or pd.isna(record['TransactionKey']):
                            from matcher_db import generate_tx_key
                            mock_row = {
                                '摘要': record['Summary'],
                                '金額': record['Amount'],
                                '年': pd.to_datetime(record['Date']).year,
                                '月': pd.to_datetime(record['Date']).month,
                                '日': pd.to_datetime(record['Date']).day
                            }
                            record['TransactionKey'] = generate_tx_key(pd.Series(mock_row))
                            
                        records.append(record)
                    
                    db.upsert_payments(records)
                    st.success(f"{len(records)} 件の入金履歴を同期しました。")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"同期エラー: {e}")
        except Exception as e:
            st.error(f"ファイル読み込みエラー: {e}")

    st.markdown("---")
    
    # Section 4: Invoice Generation
    st.markdown("### 4. 請求書PDF一括発行")
    st.write("滞納がある入居者の請求書を生成します。")
    
    mode = st.radio(
        "対象選択",
        ["延滞者のみ (Overdue Only)", "全員 (All)", "カスタム選択 (Custom)"],
        index=0
    )
    
    target_ids = None
    tenants_df = db.fetch_tenants() 
    
    if "カスタム" in mode and not tenants_df.empty:
        options = {f"{row['PropertyID']}: {row['Name']}": str(row['PropertyID']) for _, row in tenants_df.iterrows()}
        selected_labels = st.multiselect("出力対象を選択", list(options.keys()))
        if selected_labels:
            target_ids = [options[label] for label in selected_labels]
    
    if target_ids and len(target_ids) == 1:
        if st.button("選択物件の詳細プレビュー"):
            try:
                payments_df = db.fetch_payments()
                engine = LogicEngine(tenants_df, payments_df)
                invoices_data = engine.get_invoice_data(target_ids=target_ids, only_overdue=False)
                if invoices_data:
                    inv = invoices_data[0]
                    st.write(f"### {inv['Name']} 様のプレビュー")
                    st.write(f"未払残高合計: ¥{int(inv['TotalDue']):,}")
                    
                    st.write("#### 入金履歴 (LedgerHistory)")
                    if inv['LedgerHistory']:
                        display_df = pd.DataFrame(inv['LedgerHistory'])
                        if 'Date' in display_df.columns:
                            display_df['Date'] = display_df['Date'].astype(str)
                        st.table(display_df)
                    else:
                        st.warning("入金履歴が空です。")
                        with st.expander("デバッグ情報（データの中身を確認）"):
                            st.write("tenant_id:", inv.get('PropertyID'))
                            st.write("RawPaymentsCount:", inv.get('RawPaymentsCount', 0))
                            # Search by clean id
                            debug_raw = payments_df[payments_df['PropertyID'].astype(str).str.split('.').str[0] == str(inv.get('PropertyID'))]
                            st.write("マッチした生データ:", debug_raw)
                        
                    st.write("#### 請求内訳 (History)")
                    hist_df = pd.DataFrame(inv['History'])
                    if not hist_df.empty:
                        # Format numeric columns for History table
                        for col in ['amount', 'paid']:
                            if col in hist_df.columns:
                                hist_df[col] = hist_df[col].apply(lambda x: f"{int(x):,}")
                        st.table(hist_df)
                else:
                    st.warning("選択された物件のデータが見つかりませんでした。物件番号の不一致や、対象外（別口座管理など）の可能性があります。")
            except Exception as e:
                st.error(f"プレビューエラー: {e}")

    if st.button("請求書生成開始"):
        try:
            from invoice_generator_web import generate_invoice_pdf
            import zipfile
            import io
            
            payments_df = db.fetch_payments()
            engine = LogicEngine(tenants_df, payments_df)
            
            invoices_data = engine.get_invoice_data(target_ids=target_ids, only_overdue=("延滞者" in mode))
            
            if not invoices_data:
                st.info("対象となる物件（滞納など）が見つかりませんでした。")
            else:
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    for inv in invoices_data:
                        pdf_bytes = generate_invoice_pdf(inv)
                        filename = f"invoice_{inv['PropertyID']}_{datetime.now().strftime('%Y%m%d')}.pdf"
                        zf.writestr(filename, pdf_bytes)
                
                zip_buffer.seek(0)
                st.success(f"{len(invoices_data)} 件の請求書を生成しました。")
                st.download_button(
                    label="請求書ZIPをダウンロード",
                    data=zip_buffer,
                    file_name=f"invoices_{datetime.now().strftime('%Y%m%d')}.zip",
                    mime="application/zip"
                )
        except Exception as e:
            st.error(f"請求書生成エラー: {e}")

    st.markdown("---")
    
    # Section 5: Status Report
    st.markdown("### 5. 家賃滞納・入金状況（最終確認）")
    if st.button("計算・レポート表示"):
        try:
            tenants_df = db.fetch_tenants()
            payments_df = db.fetch_payments()
            engine = LogicEngine(tenants_df, payments_df)
            status_df = engine.process_status()
            
            def highlight_status(val):
                return f"color: {'red' if val == '滞納あり' else 'green'}"
            
            # Format numeric columns for clean display
            status_display = status_df.copy()
            for col in ['Rent', 'BalanceDue']:
                 if col in status_display.columns:
                     status_display[col] = status_display[col].apply(lambda x: f"{int(x):,}")
            
            st.dataframe(
                status_display.style.map(highlight_status, subset=['Status']),
                use_container_width=True,
                column_config={
                    "DEBUG_OK": "最新メモの内容",
                    "DEBUG_MGMT": "別口座管理フラグ"
                }
            )
        except Exception as e:
            st.error(f"計算エラー: {e}")
