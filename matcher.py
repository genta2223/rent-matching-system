import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import hashlib

def parse_japanese_era(date_str):
    if not isinstance(date_str, str) or not date_str.strip() or date_str.lower() == 'nan':
        return pd.NaT
    
    date_str = date_str.strip().replace(" ", "")
    # Handle H31.2.15, R5.6.1, etc.
    era_map = {'H': 1988, 'R': 2018, 'S': 1925, 'T': 1911}
    first_char = date_str[0].upper()
    
    if first_char in era_map:
        import re
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
    raw = f"{row['取扱日付　年']}{row['取扱日付　月']}{row['取扱日付　日']}{row['摘要']}{row['金額']}"
    return hashlib.md5(raw.encode('cp932', errors='replace')).hexdigest()

class TenantRecord:
    def __init__(self, data):
        self.property_id = str(data.get('PropertyID', ''))
        self.name = data.get('TenantName', '')
        self.rent = float(data.get('MonthlyRent', 0))
        candidates_raw = [normalize_name(data.get(f'BankMatchName{i}')) for i in range(1, 4)]
        self.candidates = [c for c in candidates_raw if c]
        self.initial_date = parse_japanese_era(data.get('InitialPaymentDate'))
        if pd.isna(self.initial_date) or not self.initial_date:
            # Fallback to a very old date if not specified
            self.initial_date = pd.Timestamp(year=2000, month=1, day=1)
            
        self.zip = str(data.get('Zip', ""))
        self.address = data.get('Address', "")
        
        # v4 new fields
        self.billing_zip = str(data.get('BillingAddressZip', ""))
        self.billing_address = data.get('BillingAddress', "")
        
        # Robust check for '1' or '1.0'
        raw_mgmt = str(data.get('SeparateAccountManagement', '0')).strip().lower()
        self.separate_mgmt = (raw_mgmt.startswith('1') or raw_mgmt == '1.0')
        
        self.memo = data.get('Memo', "")
        self.delinquency_memo = str(data.get('LatestPaymentMemo', ""))
        self.memo_anchor_date = None
        
        self.ledger_payments = []
        self.debts = [] # List of {month: Date, amount: Float, paid: Float}

    def calculate_debts(self, target_date):
        """Initialize debts using memo as anchor and status source."""
        # Use initial_date as the ultimate zero-point, but cap at 2 years back for safety
        # unless specifically mentioned in memo.
        limit_start = target_date.replace(day=1) - relativedelta(months=24)
        calc_start = max(self.initial_date.replace(day=1), limit_start)
        
        self.memo_anchor_date = pd.Timestamp(year=2000, month=1, day=1)
        
        memo_clean = self.delinquency_memo.strip().lower()
        is_ok = memo_clean in ['ok', '', 'nan', 'none'] or not self.delinquency_memo
        
        if is_ok:
            # Assume clean up to the end of the billing period
            self.memo_anchor_date = target_date.replace(day=1) + relativedelta(months=1)
            # Debts start from rent commencement (or capped limit)
            curr = calc_start
        else:
            import re
            # Try to find a date like 2026/01/15 in the memo
            date_match = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", self.delinquency_memo)
            if date_match:
                y, m, d = date_match.groups()
                self.memo_anchor_date = pd.Timestamp(year=int(y), month=int(m), day=int(d))
            else:
                self.memo_anchor_date = target_date.replace(day=1) - relativedelta(days=1)
            
            curr = calc_start
            # Find earliest mentioned month to potentially start earlier
            mentions = re.findall(r"(\d{4})年(\d{1,2})月", self.delinquency_memo)
            if mentions:
                timestamps = [pd.Timestamp(year=int(y), month=int(m), day=1) for y, m in mentions]
                curr = min(curr, min(timestamps))

        end_month = target_date.replace(day=1) + relativedelta(months=1)
        
        initial_paid_map = {}
        if not is_ok:
            import re
            full_paid_months = re.findall(r"(\d{4})年(\d{1,2})月分全額充当", self.delinquency_memo)
            for y, m in full_paid_months:
                initial_paid_map[pd.Timestamp(year=int(y), month=int(m), day=1)] = self.rent
            
            partial_paid = re.findall(r"(\d{4})年(\d{1,2})月分のうち(\d+)円入金", self.delinquency_memo)
            for y, m, amt in partial_paid:
                initial_paid_map[pd.Timestamp(year=int(y), month=int(m), day=1)] = float(amt)

        while curr <= end_month:
            paid_init = 0.0
            if is_ok:
                if curr <= self.memo_anchor_date:
                    paid_init = self.rent
            else:
                paid_init = initial_paid_map.get(curr, 0.0)
            
            self.debts.append({'month': curr, 'amount': self.rent, 'paid': paid_init})
            curr += relativedelta(months=1)

    def allocate_payments(self):
        """Apply ledger payments to debts using FIFO, ignoring ones before memo anchor.
        If InitialPaymentDate is available and recent, we use all known payments.
        """
        self.ledger_payments.sort(key=lambda x: x['Date'])
        
        # If we have a definite start date, we don't filter ledger payments by memo anchor.
        # This allows "reverse calculation" from rent start.
        skip_filter = (self.initial_date > pd.Timestamp(year=2010, month=1, day=1))
        
        for p in self.ledger_payments:
            p['Allocations'] = []
            p['Surplus'] = 0.0
            
            if not skip_filter and p['Date'] <= self.memo_anchor_date:
                # This payment is likely already reflected in the memo's "Paid" status.
                continue
            
            amount_to_alloc = p['Amount']
            for d in self.debts:
                if d['paid'] < d['amount']:
                    needed = d['amount'] - d['paid']
                    alloc = min(needed, amount_to_alloc)
                    if alloc > 0:
                        d['paid'] += alloc
                        amount_to_alloc -= alloc
                        p['Allocations'].append({
                            'Month': d['month'],
                            'Amount': alloc,
                            'IsFull': d['paid'] >= d['amount']
                        })
                if amount_to_alloc <= 0:
                    break
            p['Surplus'] = amount_to_alloc

    def get_total_overdue(self, limit_date):
        """Calculate total unpaid amount up to limit_date."""
        total = 0
        for d in self.debts:
            if d['month'] <= limit_date:
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
        print(f"Error loading ledger: {e}")
        ledger_df = pd.DataFrame(columns=['ID', 'PropertyID', 'PaymentDate', 'Amount', 'Summary', 'TransactionKey'])

    try:
        rent_df = pd.read_csv(rent_roll_path, encoding='utf-8-sig')
    except Exception as e:
        print(f"Error loading rent roll: {e}")
        return

    # --- Step 1: Matching and Ledger Update ---
    new_ledger_entries = []
    used_keys = set(ledger_df['TransactionKey'].tolist())
    
    # Identify transactions that match tenants and aren't in ledger
    for _, tx in bank_df.iterrows():
        tx_key = generate_tx_key(tx)
        if tx_key in used_keys: continue
        
        summary = normalize_name(str(tx['摘要']))
        amount = tx['金額']
        matched_room = None
        
        # Check against all tenants
        for _, tenant_row in rent_df.iterrows():
            cands = [normalize_name(tenant_row.get(f'BankMatchName{i}')) for i in range(1, 4)]
            if any(c in summary for c in cands if c):
                matched_room = str(tenant_row['PropertyID'])
                break
        
        if matched_room:
            # Handle potential float date columns and lead zero formatting
            try:
                y = int(tx['取扱日付　年'])
                m = int(tx['取扱日付　月'])
                d = int(tx['取扱日付　日'])
                payment_date = f"{y:04d}-{m:02d}-{d:02d}"
            except (ValueError, TypeError):
                # Fallback if parsing fails
                payment_date = datetime.now().strftime("%Y-%m-%d")

            new_entry = {
                'ID': len(ledger_df) + len(new_ledger_entries) + 1,
                'PropertyID': matched_room,
                'PaymentDate': payment_date,
                'Amount': amount,
                'Summary': tx['摘要'],
                'TransactionKey': tx_key
            }
            new_ledger_entries.append(new_entry)
            used_keys.add(tx_key)

    if new_ledger_entries:
        updated_ledger = pd.concat([ledger_df, pd.DataFrame(new_ledger_entries)], ignore_index=True)
        updated_ledger.to_csv(ledger_path, index=False, encoding='utf-8-sig')
        print(f"Added {len(new_ledger_entries)} new payments to ledger.")
        ledger_df = updated_ledger
    else:
        print("No new payments found.")

    # --- Step 2: FIFO Accounting ---
    today = datetime.now()
    # "Next month" calculation
    next_month_rent_date = (today.replace(day=1) + relativedelta(months=1))
    
    tenants = []
    for _, row in rent_df.iterrows():
        t = TenantRecord(row)
        if t.separate_mgmt:
            print(f"Skipping Property {t.property_id} (Separate Account Management)")
            continue
            
        # Load payments from ledger
        room_payments = ledger_df[ledger_df['PropertyID'].astype(str) == t.property_id]
        for _, p in room_payments.iterrows():
            t.ledger_payments.append({'Date': pd.to_datetime(p['PaymentDate']), 'Amount': p['Amount']})
        
        t.calculate_debts(today)
        t.allocate_payments()
        tenants.append(t)

    # --- Step 3: Reporting & Invoicing ---
    from invoice_generator import create_invoice
    
    invoice_folder = os.path.join(base_dir, "invoices")
    # Clean previous results for clarity
    if os.path.exists(invoice_folder):
        import shutil
        shutil.rmtree(invoice_folder)
    os.makedirs(invoice_folder)

    results = []
    for t in tenants:
        # Total due for billing includes next month
        total_due = t.get_total_overdue(next_month_rent_date)
        
        # Delinquency status only considers up to current month (Feb)
        delinquency = t.get_total_overdue(today.replace(day=1))
        
        status = 'Overdue' if delinquency > 10 else 'Paid'
        
        results.append({
            'PropertyID': t.property_id,
            'Name': t.name,
            'MonthlyRent': t.rent,
            'BalanceDue': total_due,
            'Status': status
        })
        
        # Trigger Invoice only for those actually delinquent
        if delinquency > 0:
            # Prepare 6 months history (Debts)
            history = t.debts[-7:] 
            
            # Prepare Ledger History with allocation details
            ledger_history_items = []
            # Take last 6 payments that have been allocated
            for lp in t.ledger_payments[-6:]:
                alloc_desc = []
                for a in lp['Allocations']:
                    m_str = a['Month'].strftime('%Y年%m月分')
                    type_str = "全額" if a['IsFull'] else "一部"
                    alloc_desc.append(f"{m_str}{type_str}")
                
                if lp['Surplus'] > 0:
                    alloc_desc.append(f"余剰金 {int(lp['Surplus'])}円")
                
                ledger_history_items.append({
                    'Date': lp['Date'],
                    'Amount': lp['Amount'],
                    'AllocationDesc': " / ".join(alloc_desc) if alloc_desc else "充当先なし"
                })

            tenant_info = {
                'Zip': str(t.billing_zip if t.billing_zip and t.billing_zip != "nan" else t.zip),
                'Address': str(t.billing_address if t.billing_address and t.billing_address != "nan" else t.address),
                'Name': str(t.name),
                'PropertyID': str(t.property_id),
                'TotalDue': total_due,
                'History': history, # Monthly Status
                'LedgerHistory': ledger_history_items # Payments & Allocations
            }
            invoice_name = f"invoice_{t.property_id}_{today.strftime('%Y%m%d')}.pdf"
            invoice_path = os.path.join(invoice_folder, invoice_name)
            create_invoice(tenant_info, invoice_path)
            print(f"Generated invoice for Property {t.property_id}: {invoice_name}")

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print("\n--- Current Status Report ---")
    print(results_df)

if __name__ == "__main__":
    base_dir = "private_data"
    bank_csv = None
    if os.path.exists(base_dir):
        csv_files = [f for f in os.listdir(base_dir) if f.endswith('.csv') and not f.startswith('reconciliation') and f != 'rent_roll.csv' and f != 'payment_ledger.csv']
        if csv_files:
            bank_csv = os.path.join(base_dir, sorted(csv_files, reverse=True)[0])
    
    run_matching(
        bank_csv if bank_csv else "mock",
        os.path.join(base_dir, "rent_roll.csv"),
        os.path.join(base_dir, "payment_ledger.csv"),
        os.path.join(base_dir, "reconciliation_report.csv")
    )
