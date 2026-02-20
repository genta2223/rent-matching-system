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
                
        # New Fixed Base Date Logic
        # Parse from Values JSON, with defaults
        
        # 1. Base Date (基準日)
        bd_str = values.get('base_date')
        if not bd_str or bd_str.lower() in ('none', 'nan'):
            bd_str = str(data.get('BaseDebtDate', ''))
        if not bd_str or bd_str.lower() in ('none', 'nan'):
            bd_str = '2026-02-13'
        self.base_date = pd.to_datetime(bd_str)
        if pd.isna(self.base_date):
            self.base_date = pd.Timestamp(year=2026, month=2, day=13)
            
        # 2. Base Debt (基準日時点の延滞金)
        try:
            val_debt = float(values.get('base_debt', 0))
            if val_debt == 0:
                val_debt = float(data.get('BaseDebtAmount', 0))
            self.base_debt = val_debt
        except:
            self.base_debt = 0.0
            
        # 3. Base Surplus (基準日時点の預り金)
        try:
            self.base_surplus = float(values.get('base_surplus', 0))
        except:
            self.base_surplus = 0.0
            
        # 4. Manual Adjustment (手動調整金)
        try:
            self.manual_adjustment = float(values.get('manual_adjustment', 0))
        except:
            self.manual_adjustment = 0.0
            
        # 5. Adjustment Memo (調整内容のメモ)
        self.adjustment_memo = str(values.get('adjustment_memo', ''))
        
        # 6. is_clean_start (前月末完済済フラグ)
        is_clean_raw = values.get('is_clean_start')
        if is_clean_raw is not None:
            if isinstance(is_clean_raw, str):
                self.is_clean_start = is_clean_raw.lower() in ('true', '1', 't', 'y', 'yes')
            else:
                self.is_clean_start = bool(is_clean_raw)
        else:
            self.is_clean_start = (self.base_debt <= 0)
            
        # 7. last_confirmed_date (前回確認日)
        lcd_str = values.get('last_confirmed_date', '')
        self.last_confirmed_date = pd.to_datetime(lcd_str) if lcd_str else pd.NaT
        
        self.ledger_payments = []
        self.debts = [] 

    def calculate_debts(self, target_date):
        """Initialize debts using the new Fixed Base Date Logic (基準日固定方式)."""
        target_normalized = normalize_month(target_date)
        
        # Calculate initial debt from base_debt - base_surplus
        # If this is positive, it's a debt we need to collect.
        initial_balance = self.base_debt - self.base_surplus
        if self.is_clean_start:
            initial_balance = 0.0 - self.base_surplus
        
        # Determine the month for the carry-over balance (usually the month before base_date)
        # and the month to start charging regular rent
        
        if self.base_date.day == 1:
            start_month = normalize_month(self.base_date)
            carry_month = start_month - relativedelta(months=1)
        else:
            # If base date is mid-month (e.g. Feb 13), the carry-over balance
            # represents unpaid rent up to January. 
            # Regular rent generation should start from the month of the base date (Feb).
            start_month = normalize_month(self.base_date)
            carry_month = start_month - relativedelta(months=1)
        
        # Add a single entry for the carry-over balance if it's strictly > 0.
        if initial_balance > 0:
            self.debts.append({
                'month': carry_month, 
                'amount': initial_balance, 
                'paid': 0.0,
                'is_carry_over': True
            })
            
        # Add manual adjustment if it's positive (treat as generic extra debt, like repairs)
        # If manual_adjustment is negative, it's a discount, so we inject it as a "virtual payment" later.
        if self.manual_adjustment > 0:
             self.debts.append({
                 'month': carry_month,
                 'amount': self.manual_adjustment,
                 'paid': 0.0,
                 'is_carry_over': True,
                 'is_manual_adjustment': True # Used for description
             })

        # Generate monthly rent debts from start_month up to target + 1 month
        # Ex: If target is Feb 20, we generate up to March.
        curr = start_month
        limit_end = target_normalized + relativedelta(months=1)
        while curr <= limit_end:
            self.debts.append({'month': curr, 'amount': self.rent, 'paid': 0.0, 'is_carry_over': False})
            curr += relativedelta(months=1)
        
    def allocate_payments(self):
        """FIFO allocation: payments after confirmed date get full FIFO allocation.
        Payments before confirmed date are skipped."""
        self.ledger_payments.sort(key=lambda x: x['Date'])
        
        # Determine cutoff date: use last_confirmed_date if valid, else base_date
        if pd.notna(self.last_confirmed_date):
            cutoff_date = self.last_confirmed_date.date()
        elif self.is_clean_start:
            # Clean start tenants usually pay current month rent in late previous month.
            # Set cutoff to 15th of the previous month so we capture late-Jan payments for Feb rent.
            start_m = normalize_month(self.base_date)
            cutoff_date = (start_m - pd.Timedelta(days=15)).date()
        else:
            cutoff_date = self.base_date.date()

        # Step 1: Pre-fill virtual payments
        virtual_surplus = 0.0
        
        if self.is_clean_start:
            if self.base_surplus > 0:
                virtual_surplus += self.base_surplus
        else:
            if self.base_surplus > self.base_debt:
                virtual_surplus += (self.base_surplus - self.base_debt)
            
        # If manual adjustment is negative, it's a discount, so we get virtual cash to pay debts.
        if self.manual_adjustment < 0:
            virtual_surplus += abs(self.manual_adjustment)

        # Allocate virtual surplus to debts
        if virtual_surplus > 0:
            for d in self.debts:
                if float(d['paid']) < float(d['amount']):
                    needed = float(d['amount']) - float(d['paid'])
                    alloc = min(needed, virtual_surplus)
                    if alloc > 0:
                        d['paid'] = float(d['paid']) + alloc
                        virtual_surplus -= alloc
                if virtual_surplus <= 0:
                    break

        # Step 2: Allocate actual ledger payments
        for p in self.ledger_payments:
            p['Allocations'] = []
            p['Surplus'] = 0.0
            p['AllocationDesc'] = ""

            # Skip payments on or before cutoff date
            if p['Date'].date() <= cutoff_date:
                p['AllocationDesc'] = f"確認済({cutoff_date.strftime('%Y-%m-%d')})以前の入金"
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
                        
                        if d.get('is_manual_adjustment'):
                            desc_month = "手動調整分"
                        elif d.get('is_carry_over'):
                            desc_month = f"基準日残高(〜{ts.strftime('%Y年%m月分')})"
                        else:
                            desc_month = ts.strftime('%Y年%m月分')
                            
                        type_str = "全額" if is_full else "一部"
                        p['Allocations'].append({'Month': d['month'], 'Amount': alloc, 'IsFull': is_full})
                        alloc_parts.append(f"{desc_month}{type_str}({int(alloc):,}円)")
                if amount_to_alloc <= 0:
                    break
                    
            p['Surplus'] = amount_to_alloc
            if amount_to_alloc > 0:
                alloc_parts.append(f"余剰金 {int(amount_to_alloc):,}円")
            
            # Format update
            base_desc = " / ".join(alloc_parts) if alloc_parts else "充当先なし"
            date_str = p['Date'].strftime('%Y-%m-%d')
            p['AllocationDesc'] = f"入金日:{date_str} {base_desc}"


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
                target_overdue_month = normalize_month(today)
                
                # If the user started clean, they are granted until the 20th to pay the current month
                if t.is_clean_start and today.day < 20:
                    target_overdue_month -= relativedelta(months=1)

                delinq = t.get_total_overdue(target_overdue_month)
                if delinq > 0:
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
            date_col = mapping.get('date')
            date_parts = mapping.get('date_parts')
            type_col = mapping.get('type')
            
            # Basic validation
            if not sender_col or not amount_col:
                continue
            # EMERGENCY FIX: Removed date mapping check to allow hardcoded fallback
            # if not date_col and not date_parts:
            #    continue
                
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
            
            # Generate tx_key
            tx_key = self._generate_flexible_tx_key(tx, mapping)
            
            # Duplicate check
            if tx_key in used_keys:
                continue
            
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
                # Extract Date - FORCE RESONA O/P/Q (Indices 14, 15, 16)
                # User Requirement: Rigidly use these columns. Ignore Mapper.
                try:
                    # REVERT TO IDEMPOTENT DATE EXTRACTION
                    # The input 'tx' is a row from the NORMALIZED dataframe (Date, Amount, Summary).
                    # 'csv_ai_mapper.py' has already used the O/P/Q fallback to create this 'Date' column.
                    # So we just trust 'Date'.
                    
                    if date_parts:
                         # This path is for raw DF (not used by app.py currently)
                         y = tx.get(date_parts['year'])
                         m = tx.get(date_parts['month'])
                         d = tx.get(date_parts['day'])
                         payment_date = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
                    elif isinstance(date_col, list):
                         val = tx.get(date_col[0])
                         payment_date = pd.to_datetime(val).strftime("%Y-%m-%d")
                    else:
                         val = tx.get(date_col)
                         payment_date = pd.to_datetime(val).strftime("%Y-%m-%d")
                         
                    print(f"DEBUG DATE (Prop {matched_room}): {payment_date} (from {val})")

                except Exception as e:
                    # CRITICAL: STRICT ERROR. NO DEFAULT TO TODAY.
                    print(f"Skipping row due to invalid date: {e}")
                    continue

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
        
        date_str = ""
        if mapping.get('date_parts'):
            dp = mapping['date_parts']
            y = str(row.get(dp['year'], ''))
            m = str(row.get(dp['month'], ''))
            d = str(row.get(dp['day'], ''))
            date_str = y + m + d
        else:
            date_cols = mapping.get('date', [])
            if isinstance(date_cols, str): date_cols = [date_cols]
            # Use safe get
            date_str = "".join([str(row.get(c, '')) for c in date_cols])
            
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
            
            # Exclude next month rent from the main display BalanceDue (use today instead of next_month)
            total_due = t.get_total_overdue(normalize_month(today))
            
            target_overdue_month = normalize_month(today)
            
            # If the user started clean, they are granted until the 20th to pay the current month
            if t.is_clean_start and today.day < 20:
                target_overdue_month -= relativedelta(months=1)
                
            delinq = t.get_total_overdue(target_overdue_month)
            status = '滞納あり' if delinq > 0 else '正常'
            
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
