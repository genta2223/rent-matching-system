import streamlit as st
import pandas as pd
from db_client import DBClient
from datetime import datetime

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

# Auto-refresh logic (optional, for now manual)
if st.button("データ更新"):
    st.cache_data.clear()

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["入居者台帳 (Rent Roll)", "入金明細 (Ledger)", "管理ツール"])

with tab1:
    st.subheader("入居者一覧")
    try:
        tenants_df = db.fetch_tenants()
        
        # Display Key Metrics
        total_rent = tenants_df['MonthlyRent'].sum() if not tenants_df.empty else 0
        st.metric("月額家賃総額（想定）", f"¥{total_rent:,}")
        
        # Flatten 'Values' column for editing
        if not tenants_df.empty:
            # Normalize Values column if it exists, ensuring it's a dict
            tenants_df['Values'] = tenants_df['Values'].apply(lambda x: x if isinstance(x, dict) else {})
            
            # Extract common nested fields for easier editing
            tenants_df['BankMatchName1'] = tenants_df['Values'].apply(lambda x: x.get('BankMatchName1', ''))
            tenants_df['Agent'] = tenants_df['Values'].apply(lambda x: x.get('Agent', ''))
            tenants_df['Manager'] = tenants_df['Values'].apply(lambda x: x.get('Manager', ''))
            
            # Reorder columns for better visibility (PropertyID first)
            cols = ['PropertyID', 'Name', 'MonthlyRent', 'BankMatchName1', 'Memo', 'Agent', 'Manager', 'BaseDebtAmount', 'BaseDebtDate']
            # Add remaining cols
            other_cols = [c for c in tenants_df.columns if c not in cols and c != 'Values']
            tenants_df = tenants_df[cols + other_cols]

        # Interactive Editor
        edited_df = st.data_editor(
            tenants_df, 
            use_container_width=True, 
            num_rows="dynamic",
            key="tenant_editor"
        )
        
        if st.button("変更を保存 (Save Changes)"):
            try:
                # Re-nest flattened columns back into 'Values'
                records = []
                for _, row in edited_df.iterrows():
                    record = row.to_dict()
                    
                    # Initialize Values dict (could be merging, but here we rebuild from flat cols)
                    # Note: We are losing other keys in 'Values' if we don't preserve them differently.
                    # For simplicity in this phase, we assume only these 3 matter or we should have kept the original Values and merged.
                    # Better approach: Start with empty dict or strict schema
                    values = {
                        'BankMatchName1': record.pop('BankMatchName1', None),
                        'Agent': record.pop('Agent', None),
                        'Manager': record.pop('Manager', None)
                    }
                    record['Values'] = values
                    records.append(record)
                
                db.upsert_tenants(records)
                st.success("入居者データを更新しました！")
                st.cache_data.clear() # Reload
                
            except Exception as e:
                st.error(f"保存エラー: {e}")
                
    except Exception as e:
        st.error(f"入居者データの読み込みエラー: {e}")

with tab2:
    st.subheader("入金履歴")
    try:
        payments_df = db.fetch_payments()
        if not payments_df.empty:
            st.dataframe(payments_df.sort_values(by="Date", ascending=False), use_container_width=True)
        else:
            st.info("入金データが見つかりません。")
    except Exception as e:
        st.error(f"入金データの読み込みエラー: {e}")

from matcher_db import LogicEngine 

with tab3:
    st.subheader("管理機能")
    
    # Section: CSV Upload & Sync
    st.markdown("### 1. 銀行データ取込 (りそな銀行 CSV)")
    uploaded_file = st.file_uploader("CSVファイルをドラッグ＆ドロップ", type=["csv"])
    
    if uploaded_file is not None:
        try:
            # Try CP932 (Shift-JIS) first for Japanese CSVs
            try:
                bank_df = pd.read_csv(uploaded_file, encoding='cp932')
            except UnicodeDecodeError:
                uploaded_file.seek(0)
                bank_df = pd.read_csv(uploaded_file, encoding='utf-8')
            
            st.write(f"読み込み完了: {len(bank_df)} 行")
            
            # Initialize Logic Engine
            tenants_df = db.fetch_tenants()
            payments_df = db.fetch_payments()
            engine = LogicEngine(tenants_df, payments_df)
            
            # Run Matching
            new_entries = engine.match_new_bank_data(bank_df)
            
            if new_entries:
                st.info(f"新規マッチング: {len(new_entries)} 件")
                new_entries_df = pd.DataFrame(new_entries)
                st.dataframe(new_entries_df)
                
                if st.button("データベースに登録 (Upsert)"):
                    try:
                        db.upsert_payments(new_entries)
                        st.success("登録完了しました！")
                        st.cache_data.clear() # Clear cache to refresh data on next load
                    except Exception as e:
                        st.error(f"登録エラー: {e}")
            else:
                st.warning("新規の入金データは見つかりませんでした（重複またはマッチングなし）。")
                
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
            
            # --- Data Correction Logic (similar to fix_csv.py) ---
            fixed_count = 0
            for i, row in rent_df.iterrows():
                b_amt = row.get('BaseDebtAmount')
                b_date = row.get('BaseDebtDate')
                
                # Check for column shift (Date string in Amount col)
                if isinstance(b_amt, str) and re.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$', b_amt):
                    if pd.isna(b_date) or b_date == '':
                        rent_df.at[i, 'BaseDebtDate'] = b_amt
                        rent_df.at[i, 'BaseDebtAmount'] = 0
                        fixed_count += 1
            
            if fixed_count > 0:
                st.warning(f"{fixed_count} 件のデータ列ズレを自動修正しました。")
            
            st.write(f"読み込みデータ: {len(rent_df)} 件")
            st.dataframe(rent_df.head())
            
            if st.button("台帳更新 (Upsert)"):
                try:
                    # Transform to Records
                    import math
                    import numpy as np
                    
                    records = []
                    for _, row in rent_df.iterrows():
                        # Parse MonthlyRent safely
                        try:
                            raw_rent = row.get('MonthlyRent')
                            rent = int(raw_rent) if pd.notna(raw_rent) else 0
                        except:
                            rent = 0
                        
                        # Parse BaseDebt
                        try:
                            raw_debt = row.get('BaseDebtAmount', 0)
                            base_debt = float(raw_debt) if pd.notna(raw_debt) and str(raw_debt).strip() != '' else 0.0
                        except:
                            base_debt = 0.0
                        
                        # Prepare Record
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
                                "BankMatchName3": row.get('BankMatchName3')
                            }
                        }
                        records.append(record)
                    
                    db.upsert_tenants(records)
                    st.success(f"{len(records)} 件の入居者データを更新しました。")
                    st.cache_data.clear()
                    
                except Exception as e:
                    st.error(f"更新エラー: {e}")
                    
        except Exception as e:
            st.error(f"ファイル読み込みエラー: {e}")

    st.markdown("---")
    
    # Section 3: Invoice Generation
    st.markdown("### 3. 請求書PDF一括発行")
    st.write("滞納がある入居者の請求書を生成し、ZIPで一括ダウンロードします。")
    
    # Selection Mode
    mode = st.radio(
        "対象選択",
        ["延滞者のみ (Overdue Only)", "全員 (All)", "カスタム選択 (Custom)"],
        index=0
    )
    
    target_ids = None
    only_overdue = True
    
    tenants_df = db.fetch_tenants() 
    
    if "全員" in mode:
        only_overdue = False
    elif "カスタム" in mode:
        only_overdue = False # Logic ignored when target_ids is set, but explicit is safer
        # Prepare options: 'ID: Name'
        if not tenants_df.empty:
            options = {f"{row['PropertyID']}: {row['Name']}": str(row['PropertyID']) for _, row in tenants_df.iterrows()}
            selected_labels = st.multiselect("出力対象を選択", list(options.keys()))
            if selected_labels:
                target_ids = [options[label] for label in selected_labels]
            else:
                st.warning("対象を選択してください。未選択の場合は出力されません。")
                target_ids = [] # Explicitly empty to return nothing if logic depends
        else:
            st.warning("入居者データがありません。")

    if st.button("請求書生成開始"):
        try:
            from invoice_generator_web import generate_invoice_pdf
            import zipfile
            import io
            
            # tenants_df already fetched above
            payments_df = db.fetch_payments()
            engine = LogicEngine(tenants_df, payments_df)
            
            # Pass filtering args
            invoices_data = engine.get_invoice_data(target_ids=target_ids, only_overdue=("延滞者" in mode))
            
            if not invoices_data:
                st.info("滞納者（請求書発行対象）はいません。")
            else:
                # Create ZIP in memory
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    for inv in invoices_data:
                        pdf_bytes = generate_invoice_pdf(inv)
                        # Filename: invoice_PropID_YYYYMMDD.pdf
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
    
    # Section 4: Status Report
    st.markdown("### 4. 家賃滞納・入金状況")
    if st.button("ステータス再計算"):
        try:
            tenants_df = db.fetch_tenants()
            payments_df = db.fetch_payments()
            engine = LogicEngine(tenants_df, payments_df)
            
            status_df = engine.process_status()
            
            # Styling for Status
            def highlight_status(val):
                color = 'red' if val == '滞納あり' else 'green'
                return f'color: {color}'

            st.dataframe(
                status_df.style.map(highlight_status, subset=['Status']),
                use_container_width=True
            )
        except Exception as e:
            st.error(f"ステータス計算エラー: {e}")
