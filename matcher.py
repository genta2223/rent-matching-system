import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import hashlib
import re

def parse_japanese_era(date_str):
    if not isinstance(date_str, str) or not date_str.strip() or date_str.lower() == 'nan':
        return pd.NaT
    
    date_str = date_str.strip().replace(" ", "")
    # Handle H31.2.15, R5.6.1, etc.
    era_map = {'H': 1988, 'R': 2018, 'S': 1925, 'T': 1911}
    first_char = date_str[0].upper()
    
    if first_char in era_map:
        match = re.search(r'([H|R|S|T])(\d+)\.(\d+)\.(\d+)', date_str, re.I)
        if match:
            era, year, month, day = match.groups()
            gregorian_year = era_map[era.upper()] + int(year)
            return pd.Timestamp(year=gregorian_year, month=int(month), day=int(day))
    
    # Fallback to standard pandas parsing for YYYY/MM/DD or YYYY-MM-DD
    try:
        return pd.to_datetime(date_str)
    except:
        return pd.NaT

def normalize_name(name):
    if not isinstance(name, str) or name.lower() == 'nan':
        return ""
    name = name.replace(" ", "").replace("　", "")
    for prefix in ["振込　", "振込", "ﾂｲｶ", "ｻｲｿｳ"]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    return name.upper()

def generate_tx_key(row):
    """Generate a unique key for a transaction to prevent duplicates."""
    # Handle different header formats for Date
    y = row.get('取扱日付　年') or row.get('年')
    m = row.get('取扱日付　月') or row.get('月')
    d = row.get('取扱日付　日') or row.get('日')
    summary = row.get('摘要')
    amount = row.get('金額')
    raw = f"{y}{m}{d}{summary}{amount}"
    return hashlib.md5(raw.encode('cp932', errors='replace')).hexdigest()

def normalize_month(ts):
    """Normalize a timestamp to the 1st of the month at 00:00:00."""
    if pd.isna(ts): return ts
    return pd.Timestamp(year=ts.year, month=ts.month, day=1)

class TenantRecord:
    def __init__(self, data):
        self.property_id = str(data.get('PropertyID', ''))
        self.name = data.get('TenantName', '')
        self.rent = float(data.get('MonthlyRent', 0))
        candidates_raw = [normalize_name(data.get(f'BankMatchName{i}')) for i in range(1, 4)]
        self.candidates = [c for c in candidates_raw if c]
        self.initial_date = parse_japanese_era(data.get('InitialPaymentDate'))
        if pd.isna(self.initial_date) or not self.initial_date:
            self.initial_date = pd.Timestamp(year=2000, month=1, day=1)
            
        self.zip = str(data.get('Zip', ""))
        if self.zip.lower() == 'nan': self.zip = ""
        self.address = str(data.get('Address', ""))
        if self.address.lower() == 'nan': self.address = ""
        
        self.billing_zip = str(data.get('BillingAddressZip', ""))
        if self.billing_zip.lower() == 'nan': self.billing_zip = ""
        self.billing_address = str(data.get('BillingAddress', ""))
        if self.billing_address.lower() == 'nan': self.billing_address = ""
        
        raw_mgmt = str(data.get('SeparateAccountManagement', '0')).strip().lower()
        self.separate_mgmt = (raw_mgmt.startswith('1') or raw_mgmt == '1.0')
        
        self.memo = data.get('Memo', "")
        self.delinquency_memo = str(data.get('LatestPaymentMemo', ""))
        self.memo_anchor_date = None
        self.memo_paid_map = {} # month -> paid_amount
        
        # New Data-Driven Columns
        try:
            self.base_debt_amount = float(data.get('BaseDebtAmount', 0))
        except:
            self.base_debt_amount = 0.0
        self.base_debt_date = pd.to_datetime(data.get('BaseDebtDate'))
        if pd.isna(self.base_debt_date):
            self.base_debt_date = None
        
        self.ledger_payments = []
        self.debts = [] 

    def calculate_debts(self, target_date):
        """Initialize debts using BaseDebt columns if available, otherwise fallback to memo."""
        target_normalized = normalize_month(target_date)
        
        # --- PHASE 1: Try Data-Driven Calculation (v14) ---
        if self.base_debt_date:
            self.memo_anchor_date = self.base_debt_date
            # Snapshot balance as of the anchor date
            if self.base_debt_amount > 0:
                # Add historical residue as the first debt
                # Use a dummy month key that represents "Previous Balance"
                self.debts.append({
                    'month': self.base_debt_date, 
                    'amount': self.base_debt_amount, 
                    'paid': 0.0,
                    'is_carry_over': True
                })
            
            # Start accruing monthly rent from the month FOLLOWING the anchor date
            curr = normalize_month(self.base_debt_date + relativedelta(months=1))
            limit_end = target_normalized + relativedelta(months=1)
            while curr <= limit_end:
                self.debts.append({'month': curr, 'amount': self.rent, 'paid': 0.0, 'is_carry_over': False})
                curr += relativedelta(months=1)
            return

        # --- PHASE 2: Fallback to Memo-Based Parsing (Old Logic) ---
        limit_start = target_normalized - relativedelta(months=8)
        calc_start = max(normalize_month(self.initial_date), limit_start)
        
        self.memo_paid_map = {}
        first_mention = target_normalized
        
        is_ok = self.delinquency_memo.strip().lower().startswith('ok')
        if is_ok or not self.delinquency_memo:
            self.memo_anchor_date = target_normalized + relativedelta(months=1)
            checkpoint = self.memo_anchor_date
            curr = calc_start
        else:
            def parse_year_month(y_str, m_str):
                m = int(m_str)
                if y_str:
                    return pd.Timestamp(year=int(y_str), month=m, day=1)
                y = target_date.year
                if m > target_date.month + 2:
                    y -= 1
                return pd.Timestamp(year=y, month=m, day=1)

            # Isolate the latest session by splitting by text that looks like a date
            # sections[0] is often the most recent session
            sections = re.split(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})", self.delinquency_memo)
            parse_text = self.delinquency_memo
            if len(sections) >= 3:
                # Use the first date-delimited block
                parse_text = sections[0] + sections[1] + sections[2]

            full_paid_matches = re.findall(r"(?:(\d{4})年)?(\d{1,2})月分(全額|全額充当)", parse_text)
            for y_str, m_str, _ in full_paid_matches:
                t = parse_year_month(y_str, m_str)
                if t not in self.memo_paid_map:
                    self.memo_paid_map[t] = self.rent
                if t < first_mention: first_mention = t
            
            partial_paid_matches = re.findall(r"(?:(\d{4})年)?(\d{1,2})月分のうち(\d+)円", parse_text)
            for y_str, m_str, amt in partial_paid_matches:
                t = parse_year_month(y_str, m_str)
                if t not in self.memo_paid_map:
                    self.memo_paid_map[t] = float(amt)
                if t < first_mention: first_mention = t

            date_match = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", parse_text)
            if date_match:
                y, m, d = date_match.groups()
                self.memo_anchor_date = pd.Timestamp(year=int(y), month=int(m), day=int(d))
            else:
                self.memo_anchor_date = min(first_mention, target_normalized) - relativedelta(days=1)
            
            checkpoint = first_mention
            curr = min(calc_start, checkpoint)

        limit_end = target_normalized + relativedelta(months=1)
        while curr <= limit_end:
            paid_init = 0.0
            if is_ok:
                if curr <= self.memo_anchor_date:
                    paid_init = self.rent
            else:
                if curr < checkpoint:
                    paid_init = self.rent
                if curr in self.memo_paid_map:
                    paid_init = self.memo_paid_map[curr]
            
            self.debts.append({'month': curr, 'amount': self.rent, 'paid': paid_init, 'is_carry_over': False})
            curr += relativedelta(months=1)

    def allocate_payments(self):
        """Apply ledger payments to debts using FIFO, with BaseDebt cutoff or Anchor Baseline."""
        self.ledger_payments.sort(key=lambda x: x['Date'])
        
        # 1. Identify cutoff date (prioritize BaseDebtDate)
        cutoff_date = self.base_debt_date.date() if self.base_debt_date else None
        
        anchor_payment = None
        if not cutoff_date and self.memo_anchor_date:
            for p in self.ledger_payments:
                if p['Date'].date() == self.memo_anchor_date.date():
                    anchor_payment = p
                    break

        for p in self.ledger_payments:
            p['Allocations'] = []
            p['Surplus'] = 0.0
            p['AllocationDesc'] = ""
            
            # 2. Skip all payments up to cutoff (inclusive) or memo anchor (exclusive)
            if cutoff_date and p['Date'].date() <= cutoff_date:
                p['AllocationDesc'] = "記録済み"
                continue
            if not cutoff_date and self.memo_anchor_date and p['Date'].date() < self.memo_anchor_date.date():
                p['AllocationDesc'] = "記録済み"
                continue

            # 3. Case: The Anchor Payment (Memo-based logic fallback)
            if p == anchor_payment:
                alloc_parts = []
                for month, amt in sorted(self.memo_paid_map.items()):
                    if amt > 0:
                        type_str = "全額" if amt >= self.rent else "一部"
                        alloc_parts.append(f"{month.strftime('%Y年%m月分')}{type_str}({int(amt):,}円)")
                
                for month, amt in self.memo_paid_map.items():
                    p['Allocations'].append({
                        'Month': month,
                        'Amount': amt,
                        'IsFull': amt >= self.rent
                    })
                
                p['AllocationDesc'] = " / ".join(alloc_parts) if alloc_parts else "充当内容なし"
                continue

            # 4. Case: Standard FIFO for payments AFTER the cutoff/anchor
            amount_to_alloc = p['Amount']
            alloc_parts = []
            for d in self.debts:
                if d['paid'] < d['amount']:
                    needed = d['amount'] - d['paid']
                    alloc = min(needed, amount_to_alloc)
                    if alloc > 0:
                        d['paid'] += float(alloc)
                        amount_to_alloc -= float(alloc)
                        is_full = d['paid'] >= d['amount']
                        p['Allocations'].append({
                            'Month': d['month'],
                            'Amount': alloc,
                            'IsFull': is_full
                        })
                        
                        # Fix formatting for carry-over or regular month
                        if d.get('is_carry_over'):
                            desc_month = "前月以前残高"
                        else:
                            # Ensure d['month'] is Timestamp before strftime
                            ts = pd.Timestamp(d['month'])
                            desc_month = ts.strftime('%Y年%m月分')
                            
                        type_str = "全額" if is_full else "一部"
                        alloc_parts.append(f"{desc_month}{type_str}({int(alloc):,}円)")
                if amount_to_alloc <= 0:
                    break
            p['Surplus'] = amount_to_alloc
            if amount_to_alloc > 0:
                alloc_parts.append(f"余剰金 {int(amount_to_alloc):,}円")
            p['AllocationDesc'] = " / ".join(alloc_parts) if alloc_parts else "充当先なし"

    def get_total_overdue(self, limit_date):
        limit_ts = normalize_month(limit_date)
        total = 0
        for d in self.debts:
            if d['month'] <= limit_ts:
                total += (d['amount'] - d['paid'])
        return total

def run_matching(bank_csv_path, rent_roll_path, ledger_path, output_path):
    print(f"Syncing Bank Data -> Ledger...")
    try:
        try:
            bank_df = pd.read_csv(bank_csv_path, encoding='cp932')
        except UnicodeDecodeError:
            bank_df = pd.read_csv(bank_csv_path, encoding='utf-8-sig')
    except Exception as e:
        print(f"Error loading bank CSV: {e}")
        return

    try:
        if os.path.exists(ledger_path) and os.path.getsize(ledger_path) > 0:
            ledger_df = pd.read_csv(ledger_path, encoding='utf-8-sig')
        else:
            ledger_df = pd.DataFrame(columns=['ID', 'PropertyID', 'PaymentDate', 'Amount', 'Summary', 'TransactionKey'])
    except Exception as e:
        ledger_df = pd.DataFrame(columns=['ID', 'PropertyID', 'PaymentDate', 'Amount', 'Summary', 'TransactionKey'])

    try:
        rent_df = pd.read_csv(rent_roll_path, encoding='utf-8-sig')
    except Exception as e:
        print(f"Error loading rent roll: {e}")
        return

    new_ledger_entries = []
    used_keys = set(ledger_df['TransactionKey'].tolist())
    
    for _, tx in bank_df.iterrows():
        tx_key = generate_tx_key(tx)
        if tx_key in used_keys: continue
        
        summary_raw = str(tx.get('摘要', ''))
        summary = normalize_name(summary_raw)
        amount = tx.get('金額', 0)
        matched_room = None
        
        for _, tenant_row in rent_df.iterrows():
            cands = [normalize_name(tenant_row.get(f'BankMatchName{i}')) for i in range(1, 4)]
            if any(c in summary for c in cands if c):
                matched_room = str(tenant_row['PropertyID'])
                break
        
        if matched_room:
            try:
                y = tx.get('取扱日付　年') or tx.get('年')
                m = tx.get('取扱日付　月') or tx.get('月')
                d = tx.get('取扱日付　日') or tx.get('日')
                payment_date = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
            except (ValueError, TypeError):
                payment_date = datetime.now().strftime("%Y-%m-%d")

            new_ledger_entries.append({
                'ID': len(ledger_df) + len(new_ledger_entries) + 1,
                'PropertyID': matched_room,
                'PaymentDate': payment_date,
                'Amount': amount,
                'Summary': summary_raw,
                'TransactionKey': tx_key
            })
            used_keys.add(tx_key)

    if new_ledger_entries:
        updated_ledger = pd.concat([ledger_df, pd.DataFrame(new_ledger_entries)], ignore_index=True)
        updated_ledger.to_csv(ledger_path, index=False, encoding='utf-8-sig')
        print(f"Added {len(new_ledger_entries)} new payments to ledger.")
        ledger_df = updated_ledger
    else:
        print("No new payments found.")

    today = datetime.now()
    next_month_rent_date = normalize_month(today + relativedelta(months=1))
    
    tenants = []
    for _, row in rent_df.iterrows():
        t = TenantRecord(row)
        if t.separate_mgmt: continue
            
        room_payments = ledger_df[ledger_df['PropertyID'].astype(str) == t.property_id]
        for _, p in room_payments.iterrows():
            t.ledger_payments.append({'Date': pd.to_datetime(p['PaymentDate']), 'Amount': p['Amount']})
        
        t.calculate_debts(today)
        t.allocate_payments()
        tenants.append(t)

    from invoice_generator import create_invoice
    invoice_folder = os.path.join("private_data", "invoices")
    if os.path.exists(invoice_folder):
        import shutil
        shutil.rmtree(invoice_folder)
    os.makedirs(invoice_folder)

    results = []
    for t in tenants:
        total_due = t.get_total_overdue(next_month_rent_date)
        delinquency = t.get_total_overdue(normalize_month(today))
        status = 'Overdue' if delinquency > 10 else 'Paid'
        
        results.append({
            'PropertyID': t.property_id,
            'Name': t.name,
            'Rent': t.rent,
            'Balance Due': total_due,
            'Status': status
        })
        
        if total_due > 10:
            history = t.debts[-7:] 
            ledger_history_items = []
            for lp in t.ledger_payments[-6:]:
                if not lp.get('Allocations'): continue
                alloc_desc = []
                for a in lp['Allocations']:
                    m_str = a['Month'].strftime('%Y年%m月分')
                    type_str = "全額" if a['IsFull'] else "一部"
                    alloc_desc.append(f"{m_str}{type_str}({int(a['Amount']):,}円)")
                
                if lp.get('Surplus', 0) > 0:
                    alloc_desc.append(f"余剰金 {int(lp['Surplus'])}円")
                
                ledger_history_items.append({
                    'Date': lp['Date'],
                    'Amount': lp['Amount'],
                    'AllocationDesc': " / ".join(alloc_desc) if alloc_desc else "充当先なし"
                })
            
            ledger_history_items.sort(key=lambda x: x['Date'], reverse=True)

            tenant_info = {
                'Zip': str(t.billing_zip if t.billing_zip and t.billing_zip != "nan" else t.zip),
                'Address': str(t.billing_address if t.billing_address and t.billing_address != "nan" else t.address),
                'Name': str(t.name),
                'PropertyID': str(t.property_id),
                'TotalDue': total_due,
                'History': history,
                'LedgerHistory': ledger_history_items
            }
            invoice_name = f"invoice_{t.property_id}_{today.strftime('%Y%m%d')}.pdf"
            invoice_path = os.path.join(invoice_folder, invoice_name)
            create_invoice(tenant_info, invoice_path)
            print(f"Generated invoice for Property {t.property_id}")

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    # --- Step 4: Persistent Debt History Tracking ---
    debt_history = []
    for t in tenants:
        for d in t.debts:
            debt_history.append({
                'PropertyID': t.property_id,
                'Name': t.name,
                'Month': d['month'].strftime('%Y-%m'),
                'RentAmount': d['amount'],
                'PaidAmount': d['paid'],
                'Balance': d['amount'] - d['paid']
            })
    
    if debt_history:
        history_path = os.path.join(base_dir, "debt_history.csv")
        pd.DataFrame(debt_history).to_csv(history_path, index=False, encoding='utf-8-sig')
        print(f"Exported persistent debt history to {history_path}")

    # Mask names for terminal display to protect privacy
    display_df = results_df.copy()
    display_df['Name'] = display_df['Name'].apply(lambda x: x[0] + "◯" * (len(x)-1) if x else x)
    print("\n--- Current Status Report ---")
    print(display_df)

if __name__ == "__main__":
    base_dir = "private_data"
    bank_csv = None
    if os.path.exists(base_dir):
        csv_files = [f for f in os.listdir(base_dir) if f.endswith('.csv') and not f.startswith('reconciliation') and f != 'rent_roll.csv' and f != 'payment_ledger.csv' and len(f) > 15]
        if csv_files:
            bank_csv = os.path.join(base_dir, sorted(csv_files, reverse=True)[0])
    
    run_matching(
        bank_csv if bank_csv else os.path.join(base_dir, "bank_data.csv"),
        os.path.join(base_dir, "rent_roll.csv"),
        os.path.join(base_dir, "payment_ledger.csv"),
        "report_status.csv"
    )
