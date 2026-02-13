import pandas as pd
import hashlib
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta

def parse_japanese_era(date_str):
    if not isinstance(date_str, str) or not date_str.strip() or date_str.lower() == 'nan':
        return pd.NaT
    date_str = date_str.strip().replace(" ", "")
    era_map = {'H': 1988, 'R': 2018, 'S': 1925, 'T': 1911}
    first_char = date_str[0].upper()
    if first_char in era_map:
        match = re.search(r'([H|R|S|T])(\d+)\.(\d+)\.(\d+)', date_str, re.I)
        if match:
            era, year, month, day = match.groups()
            gregorian_year = era_map[era.upper()] + int(year)
            return pd.Timestamp(year=gregorian_year, month=int(month), day=int(day))
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
    y = row.get('取扱日付　年') or row.get('年')
    m = row.get('取扱日付　月') or row.get('月')
    d = row.get('取扱日付　日') or row.get('日')
    summary = row.get('摘要')
    amount = row.get('金額')
    raw = f"{y}{m}{d}{summary}{amount}"
    return hashlib.md5(raw.encode('cp932', errors='replace')).hexdigest()

def normalize_month(ts):
    if pd.isna(ts): return ts
    return pd.Timestamp(year=ts.year, month=ts.month, day=1)

class TenantRecordDB:
    def __init__(self, data):
        # Expecting Supabase column names
        self.property_id = str(data.get('PropertyID', ''))
        self.name = data.get('Name', '') # Changed from TenantName
        self.rent = float(data.get('MonthlyRent', 0))
        
        # Handle Values JSONB unpacking
        values = data.get('Values', {})
        if not isinstance(values, dict):
            values = {} # Handle None or malformed
            
        candidates_raw = [
            normalize_name(values.get('BankMatchName1')),
            normalize_name(values.get('BankMatchName2')), 
            normalize_name(values.get('BankMatchName3'))
        ]
        self.candidates = [c for c in candidates_raw if c]
        
        self.initial_date = parse_japanese_era(data.get('InitialPaymentDate'))
        if pd.isna(self.initial_date):
            self.initial_date = pd.Timestamp(year=2000, month=1, day=1)
            
        self.zip = str(data.get('Zip', ""))
        self.address = str(data.get('Address', ""))
        
        # Access nested values if they exist in 'Values' or top level if migrated differently
        # For now assuming 'Values' holds the extra fields as per migration script
        self.agent = values.get('Agent', '')
        self.manager = values.get('Manager', '')
        
        self.memo = data.get('Memo', "")
        
        raw_delinq = data.get('LatestPaymentMemo')
        if raw_delinq is None:
            self.delinquency_memo = ""
        else:
            self.delinquency_memo = str(raw_delinq)
            if self.delinquency_memo.lower() == 'nan' or self.delinquency_memo.lower() == 'none':
                self.delinquency_memo = ""
                
        self.memo_anchor_date = None
        self.memo_paid_map = {}
        
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
        target_normalized = normalize_month(target_date)
        if self.base_debt_date:
            self.memo_anchor_date = self.base_debt_date
            if self.base_debt_amount > 0:
                self.debts.append({
                    'month': self.base_debt_date, 
                    'amount': self.base_debt_amount, 
                    'paid': 0.0,
                    'is_carry_over': True
                })
            curr = normalize_month(self.base_debt_date + relativedelta(months=1))
            limit_end = target_normalized + relativedelta(months=1)
            while curr <= limit_end:
                self.debts.append({'month': curr, 'amount': self.rent, 'paid': 0.0, 'is_carry_over': False})
                curr += relativedelta(months=1)
            return

        # --- PHASE 2: Fallback to Memo-Based Parsing (Restored Logic) ---
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
                # If month is significantly ahead of current month, assume previous year (e.g. reading Dec in Jan)
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
        self.ledger_payments.sort(key=lambda x: x['Date'])
        # Simplified allocation logic prioritizing BaseDebtDate
        cutoff_date = self.base_debt_date.date() if self.base_debt_date else None
        
        for p in self.ledger_payments:
            p['Allocations'] = []
            p['Surplus'] = 0.0
            p['AllocationDesc'] = ""
            
            if cutoff_date and p['Date'].date() <= cutoff_date:
                p['AllocationDesc'] = "記録済み"
                continue

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
                        ts = pd.Timestamp(d['month'])
                        desc_month = "前月以前残高" if d.get('is_carry_over') else ts.strftime('%Y年%m月分')
                        type_str = "全額" if is_full else "一部"
                        p['Allocations'].append({'Month': d['month'], 'Amount': alloc, 'IsFull': is_full})
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

    def to_invoice_dict(self):
        today = datetime.now()
        next_month = normalize_month(today + relativedelta(months=1))
        total_due = self.get_total_overdue(next_month)
        
        # Format History
        history = []
        for d in self.debts:
             history.append({
                 'month': d['month'],
                 'amount': d['amount'],
                 'paid': d['paid']
             })
        
        # Format Ledger History
        ledger_hist = []
        for lp in self.ledger_payments[-6:]: # Last 6 payments
             if not lp.get('Allocations'): continue
             ledger_hist.append({
                 'Date': lp['Date'],
                 'Amount': lp['Amount'],
                 'AllocationDesc': lp.get('AllocationDesc', '')
             })
        ledger_hist.sort(key=lambda x: x['Date'], reverse=True)
         
        return {
            'Zip': self.zip,
            'Address': self.address,
            'Name': self.name,
            'PropertyID': self.property_id,
            'TotalDue': total_due,
            'History': history[-12:], # Last 12 months debt history
            'LedgerHistory': ledger_hist
        }

class LogicEngine:
    def __init__(self, tenants_df, ledger_df):
        self.tenants_df = tenants_df
        self.ledger_df = ledger_df
        
    def get_invoice_data(self, target_ids=None, only_overdue=True):
        """
        Returns list of invoice dicts.
        Args:
            target_ids (list): List of PropertyIDs to force include.
            only_overdue (bool): If True, only include tenants with overdue > 10.
                                 Ignored if target_ids is provided.
        """
        today = datetime.now()
        invoices = []
        
        ledger_records = self.ledger_df.to_dict('records')
        
        for _, row in self.tenants_df.iterrows():
            t = TenantRecordDB(row)
            
            # Skip separate management properties (e.g. Prop 11)
            if t.separate_mgmt:
                continue
                
            room_payments = [p for p in ledger_records if str(p.get('PropertyID')) == t.property_id]
            for p in room_payments:
                t.ledger_payments.append({'Date': pd.to_datetime(p['Date']), 'Amount': p['Amount']})
            
            t.calculate_debts(today)
            t.allocate_payments()
            
            # Filtering Logic
            is_target = False
            
            if target_ids is not None:
                # Custom selection mode
                if t.property_id in target_ids:
                    is_target = True
            elif only_overdue:
                # Overdue only mode
                delinq = t.get_total_overdue(normalize_month(today))
                if delinq > 10:
                    is_target = True
            else:
                # All mode
                is_target = True
            
            if is_target:
                invoices.append(t.to_invoice_dict())
                
        return invoices

    def match_new_bank_data(self, bank_df):
        """
        Matches bank CSV data to tenants using the logic.
        Returns:
            new_ledger_entries (list of dict): Rows to be added AND inserted to DB.
        """
        new_ledger_entries = []
        used_keys = set(self.ledger_df['TransactionKey'].tolist()) if 'TransactionKey' in self.ledger_df.columns else set()
        
        for _, tx in bank_df.iterrows():
            tx_key = generate_tx_key(tx)
            if tx_key in used_keys: continue
            
            summary_raw = str(tx.get('摘要', ''))
            summary = normalize_name(summary_raw)
            amount = tx.get('金額', 0)
            matched_room = None
            
            # Match Logic
            for _, row in self.tenants_df.iterrows():
                # Values is dict if loaded from DB
                values = row.get('Values', {})
                if not isinstance(values, dict): values = {}
                cands = [
                    normalize_name(values.get('BankMatchName1')),
                    normalize_name(values.get('BankMatchName2')),
                    normalize_name(values.get('BankMatchName3'))
                ]
                if any(c in summary for c in cands if c):
                    matched_room = str(row['PropertyID'])
                    break
            
            if matched_room:
                try:
                    y = tx.get('取扱日付　年') or tx.get('年')
                    m = tx.get('取扱日付　月') or tx.get('月')
                    d = tx.get('取扱日付　日') or tx.get('日')
                    payment_date = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
                except:
                    payment_date = datetime.now().strftime("%Y-%m-%d")

                new_ledger_entries.append({
                    'PropertyID': matched_room,
                    'Date': payment_date, # DB column is Date, originally PaymentDate in CSV. DB schema uses "Date"
                    'Amount': amount,
                    'Summary': summary_raw,
                    'TransactionKey': tx_key
                })
                used_keys.add(tx_key)
                
        return new_ledger_entries

    def process_status(self):
        """
        Calculates current status for all tenants based on loaded ledger.
        """
        today = datetime.now()
        next_month = normalize_month(today + relativedelta(months=1))
        
        results = []
        
        # Convert ledger_df to list of dicts for faster access
        ledger_records = self.ledger_df.to_dict('records')
        
        for _, row in self.tenants_df.iterrows():
            t = TenantRecordDB(row)
            
            # Filter payments for this tenant
            # Ensure type matching for PropertyID
            room_payments = [p for p in ledger_records if str(p.get('PropertyID')) == t.property_id]
            
            for p in room_payments:
                t.ledger_payments.append({'Date': pd.to_datetime(p['Date']), 'Amount': p['Amount']})
            
            t.calculate_debts(today)
            t.allocate_payments()
            
            total_due = t.get_total_overdue(next_month)
            delinq = t.get_total_overdue(normalize_month(today))
            status = '滞納あり' if delinq > 10 else '正常'
            
            results.append({
                'PropertyID': t.property_id,
                'Name': t.name,
                'Rent': t.rent,
                'BalanceDue': total_due,
                'Status': status,
                'LastAlloc': t.ledger_payments[-1]['AllocationDesc'] if t.ledger_payments else ""
            })
            
        return pd.DataFrame(results)
