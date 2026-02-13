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

def clean_pid(val):
    if pd.isna(val): return ""
    v = str(val).split('.')[0] # Remove .0
    return v.strip() if v else ""

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
        
        self.agent = values.get('Agent', '')
        self.manager = values.get('Manager', '')

        # Handle separate management flag
        raw_mgmt = data.get('SeparateAccountManagement')
        if raw_mgmt is None:
            raw_mgmt = values.get('SeparateAccountManagement', '0')
        
        try:
            # Handles 1, 1.0, "1", "1.0", True, "true"
            f_val = float(raw_mgmt)
            self.separate_mgmt = (f_val == 1.0)
        except:
            raw_mgmt_str = str(raw_mgmt).strip().lower()
            self.separate_mgmt = (raw_mgmt_str.startswith('1') or raw_mgmt_str == 'true')
        
        raw_memo = data.get('Memo')
        self.memo = str(raw_memo) if raw_memo is not None else ""
        
        raw_delinq = data.get('LatestPaymentMemo')
        self.delinquency_memo = str(raw_delinq) if raw_delinq is not None else ""
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
        """Initialize debts using BaseDebt columns if available, otherwise fallback to memo."""
        target_normalized = normalize_month(target_date)
        
        # Prioritize "ok" memo if it exists and is fresh
        is_ok = self.delinquency_memo.strip().lower().startswith('ok')
        if is_ok:
             # If "ok", treat as paid up to next month
             self.memo_anchor_date = target_normalized + relativedelta(months=1)
             limit_start = target_normalized - relativedelta(months=8)
             calc_start = max(normalize_month(self.initial_date), limit_start)
             curr = calc_start
             limit_end = target_normalized + relativedelta(months=1)
             while curr <= limit_end:
                 # Everything up to anchor is paid
                 paid_init = self.rent if curr <= self.memo_anchor_date else 0.0
                 self.debts.append({'month': curr, 'amount': self.rent, 'paid': paid_init, 'is_carry_over': False})
                 curr += relativedelta(months=1)
             return

        # --- PHASE 1: Data-Driven + Memo-Enhanced Calculation ---
        if self.base_debt_date:
            self.memo_anchor_date = self.base_debt_date
            
            # Parse LatestPaymentMemo for descriptive labels on pre-cutoff payments
            self.memo_paid_map = {}
            memo_text = self.delinquency_memo or ''
            
            # Extract first date-delimited section (most recent memo entry)
            sections = re.split(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})", memo_text)
            parse_text = memo_text
            if len(sections) >= 3:
                parse_text = sections[0] + sections[1] + sections[2]
            
            # Parse "X月分全額" or "X月分全額充当" 
            full_paid = re.findall(r"(?:(\d{4})年)?(\d{1,2})月分(?:全額|全額充当)", parse_text)
            for y_str, m_str in full_paid:
                m_val = int(m_str)
                if y_str:
                    t = pd.Timestamp(year=int(y_str), month=m_val, day=1)
                else:
                    y = target_date.year
                    if m_val > target_date.month + 2:
                        y -= 1
                    t = pd.Timestamp(year=y, month=m_val, day=1)
                self.memo_paid_map[t] = self.rent
            
            # Parse "X月分のうちN円"
            partial_paid = re.findall(r"(?:(\d{4})年)?(\d{1,2})月分のうち(\d+)円", parse_text)
            for y_str, m_str, amt in partial_paid:
                m_val = int(m_str)
                if y_str:
                    t = pd.Timestamp(year=int(y_str), month=m_val, day=1)
                else:
                    y = target_date.year
                    if m_val > target_date.month + 2:
                        y -= 1
                    t = pd.Timestamp(year=y, month=m_val, day=1)
                if t not in self.memo_paid_map:
                    self.memo_paid_map[t] = float(amt)
            
            # Debts start from BaseDebtDate's month:
            # If BaseDebtDate is on the 1st (e.g. 2025-11-01), debts start from that month (Nov)
            # If BaseDebtDate is mid/end of month (e.g. 2025-10-31), debts start from next month (Nov)
            if self.base_debt_date.day == 1:
                start_month = normalize_month(self.base_debt_date)
            else:
                start_month = normalize_month(self.base_debt_date + relativedelta(months=1))
            
            # Add carry-over debt (outstanding amount at BaseDebtDate)
            if self.base_debt_amount > 0:
                self.debts.append({
                    'month': normalize_month(self.base_debt_date),
                    'amount': self.base_debt_amount, 
                    'paid': 0.0,
                    'is_carry_over': True
                })
            
            # Generate monthly debts from start_month through next month
            curr = start_month
            limit_end = target_normalized + relativedelta(months=1)
            while curr <= limit_end:
                self.debts.append({'month': curr, 'amount': self.rent, 'paid': 0.0, 'is_carry_over': False})
                curr += relativedelta(months=1)
            return

        # --- PHASE 2: Fallback to Memo-Based Parsing ---
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
        """FIFO allocation: payments after base_debt_date get full FIFO allocation.
        Payments before base_debt_date are skipped (already covered older debts)."""
        self.ledger_payments.sort(key=lambda x: x['Date'])
        # Use base_debt_date as cutoff — payments before this date covered older months
        cutoff_date = self.base_debt_date.date() if self.base_debt_date else None

        for p in self.ledger_payments:
            p['Allocations'] = []
            p['Surplus'] = 0.0
            p['AllocationDesc'] = ""

            # Skip payments before base_debt_date (they covered months before debt start)
            if cutoff_date and p['Date'].date() < cutoff_date:
                p['AllocationDesc'] = "処理済み入金"
                continue

            # FIFO allocation for post-cutoff payments
            amount_to_alloc = float(p['Amount'])
            alloc_parts = []
            for d in self.debts:
                if float(d['paid']) < float(d['amount']):
                    needed = float(d['amount']) - float(d['paid'])
                    alloc = min(needed, amount_to_alloc)
                    if alloc > 0:
                        d['paid'] = float(d['paid']) + alloc
                        amount_to_alloc -= alloc
                        is_full = float(d['paid']) >= float(d['amount'])
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
        
        # Format History — Rule②: only show months where unpaid > 0
        #                  Rule③: include up to next month
        history = []
        for d in self.debts:
            # Skip months beyond next month
            if d['month'] > next_month:
                continue
            unpaid = float(d['amount']) - float(d['paid'])
            # Rule②: Only include months with outstanding balance
            if unpaid <= 0:
                continue
            history.append({
                'month': d['month'].strftime('%Y-%m-%d'),
                'amount': int(d['amount']),
                'paid': int(d['paid'])
            })
        
        # Format Ledger History — last 6 payments with allocation descriptions
        ledger_hist = []
        for lp in self.ledger_payments[-6:]:
             desc = str(lp.get('AllocationDesc', '') or '')
             if not desc:
                 continue
             ledger_hist.append({
                 'Date': lp['Date'],
                 'Amount': lp['Amount'],
                 'AllocationDesc': desc
             })
        return {
            'Zip': self.zip,
            'Address': self.address,
            'Name': self.name,
            'PropertyID': self.property_id,
            'TotalDue': int(total_due),
            'RawPaymentsCount': len(self.ledger_payments),
            'History': history[::-1], # Newest first
            'LedgerHistory': ledger_hist[::-1] # Newest first
        }

class LogicEngine:
    def __init__(self, tenants_df, ledger_df):
        self.tenants_df = tenants_df
        # Ensure ledger_df PropertyID is always string for matching
        if not ledger_df.empty and 'PropertyID' in ledger_df.columns:
            ledger_df = ledger_df.copy()
            ledger_df['PropertyID'] = ledger_df['PropertyID'].astype(str).str.split('.').str[0].str.strip()
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
            
            # Skip separate management properties (e.g. Prop 10, 11)
            if t.separate_mgmt:
                continue
                
            t_pid = clean_pid(t.property_id)
            # Both sides are now guaranteed to be clean strings
            room_payments = [p for p in ledger_records if str(p.get('PropertyID')) == t_pid]
            for p in room_payments:
                p_entry = p.copy()
                p_entry['Date'] = pd.to_datetime(p['Date'])
                t.ledger_payments.append(p_entry)
            
            t.calculate_debts(today)
            t.allocate_payments()
            
            # Filtering Logic
            is_target = False
            
            clean_t_pid = clean_pid(t.property_id)
            if target_ids is not None:
                # Custom selection mode - ensure target_ids are also cleaned for comparison
                clean_targets = [clean_pid(tid) for tid in target_ids] if target_ids else []
                if clean_t_pid in clean_targets:
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

    def match_new_bank_data(self, bank_df, mapping=None):
        """
        Matches bank CSV data to tenants using the logic.
        Returns:
            new_ledger_entries (list of dict): Rows to be added AND inserted to DB.
        """
        if mapping is None:
            # Fallback to Resona default if no mapping provided
            from matcher_db import BankMapper
            mapping = BankMapper.suggest_mapping(bank_df)
            
        new_ledger_entries = []
        used_keys = set(self.ledger_df['TransactionKey'].tolist()) if 'TransactionKey' in self.ledger_df.columns else set()
        
        for _, tx in bank_df.iterrows():
            # Get values based on mapping
            sender_col = mapping.get('sender')
            amount_col = mapping.get('amount')
            date_cols = mapping.get('date', [])
            type_col = mapping.get('type')
            
            # Basic validation
            if not sender_col or not amount_col or not date_cols:
                continue
                
            # Filter by type if available (e.g. only "入金")
            if type_col:
                t_val = str(tx.get(type_col, ''))
                if "入金" not in t_val and "振込" not in t_val:
                    # Heuristic: if it's not clear, we might be skipping "支払"
                    if "払" in t_val or "出" in t_val:
                        continue

            summary_raw = str(tx.get(sender_col, ''))
            summary = normalize_name(summary_raw)
            amount = tx.get(amount_col, 0)
            
            # Generate tx_key (still needs a stable way, using the raw row values for now)
            # To keep it compatible with existing Resona logic, we might need a more generic generate_tx_key
            tx_key = self._generate_flexible_tx_key(tx, mapping)
            if tx_key in used_keys: continue
            
            matched_room = None
            
            # Match Logic
            for _, row in self.tenants_df.iterrows():
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
                 # Extract Date
                 try:
                     if len(date_cols) == 3:
                         y = tx.get(date_cols[0])
                         m = tx.get(date_cols[1])
                         d = tx.get(date_cols[2])
                         payment_date = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
                     else:
                         payment_date = pd.to_datetime(tx.get(date_cols[0])).strftime("%Y-%m-%d")
                 except:
                     payment_date = datetime.now().strftime("%Y-%m-%d")

                 new_ledger_entries.append({
                     'PropertyID': matched_room,
                     'Date': payment_date,
                     'Amount': amount,
                     'Summary': summary_raw,
                     'TransactionKey': tx_key
                 })
                 used_keys.add(tx_key)
                 
        return new_ledger_entries

    def _generate_flexible_tx_key(self, row, mapping):
        # Concatenate mapping-relevant values to create a stable key
        sender = str(row.get(mapping.get('sender'), ''))
        amount = str(row.get(mapping.get('amount'), '0'))
        date_str = "".join([str(row.get(c, '')) for c in mapping.get('date', [])])
        raw = f"{date_str}{sender}{amount}"
        return hashlib.md5(raw.encode('cp932', errors='replace')).hexdigest()

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
            
            # Skip separate management
            if t.separate_mgmt:
                continue
            
            # Filter payments for this tenant
            # ledger_df PropertyID is already cleaned in __init__
            t_pid = clean_pid(t.property_id)
            room_payments = [p for p in ledger_records if str(p.get('PropertyID')) == t_pid]
            
            for p in room_payments:
                p_entry = p.copy()
                p_entry['Date'] = pd.to_datetime(p['Date'])
                t.ledger_payments.append(p_entry)
            
            t.calculate_debts(today)
            t.allocate_payments()
            
            total_due = t.get_total_overdue(next_month)
            delinq = t.get_total_overdue(normalize_month(today))
            status = '滞納あり' if delinq > 10 else '正常'
            
            results.append({
                'PropertyID': t.property_id,
                'Name': t.name,
                'Rent': int(t.rent),
                'BalanceDue': int(total_due),
                'Status': status,
                'LastAlloc': t.ledger_payments[-1]['AllocationDesc'] if t.ledger_payments else "",
                'DEBUG_OK': t.delinquency_memo,
                'DEBUG_MGMT': t.separate_mgmt
            })
            
            # Print to terminal for developer
            print(f"DEBUG: Prop {t.property_id} - Ok: {t.delinquency_memo[:10]}, Mgmt: {t.separate_mgmt}, Status: {status}")
            
        return pd.DataFrame(results)
class BankMapper:
    @staticmethod
    def suggest_mapping(df):
        cols = df.columns.tolist()
        mapping = {
            'date': [],
            'amount': None,
            'sender': None,
            'type': None
        }
        
        # Date Logic (Can be multiple columns for Resona or single)
        resona_date = ['取扱日付　年', '取扱日付　月', '取扱日付　日']
        if all(c in cols for c in resona_date):
            mapping['date'] = resona_date
        else:
            for c in cols:
                if any(k in c for k in ['日付', '日', '年月日', 'Date']):
                    mapping['date'] = [c]
                    break
        
        # Amount Logic
        for c in cols:
            if any(k in c for k in ['金額', '入金', 'Amount']):
                mapping['amount'] = c
                break
        
        # Sender Logic
        for c in cols:
            if any(k in c for k in ['摘要', '振込人', 'お名前', 'Sender']):
                mapping['sender'] = c
                break
        
        # Type Logic (to filter "入金" if present)
        for c in cols:
            if any(k in c for k in ['取引名', '区分', 'Type']):
                mapping['type'] = c
                break
                
        return mapping
