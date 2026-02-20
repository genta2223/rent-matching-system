"""
csv_ai_mapper.py — Heuristic-based bank CSV auto-detection + template management.

No LLM required. Uses pattern matching on header names and data types.
Templates are stored in Supabase (csv_templates table) for persistence.
"""

import hashlib
import json
import re
from datetime import datetime

import pandas as pd

# ---------------------------------------------------------------------------
# Constants: keyword patterns for auto-detection (Japanese bank CSVs)
# ---------------------------------------------------------------------------

DATE_KEYWORDS = ['日付', '年月日', '取引日', '振込日', '入金日', '処理日', '受付日', 'date']
DATE_COMPONENT_YEAR = ['年', 'year']
DATE_COMPONENT_MONTH = ['月', 'month']
DATE_COMPONENT_DAY = ['日', 'day']

AMOUNT_KEYWORDS = ['金額', '入金額', '振込金額', '取引金額', 'お支払金額', 'amount']
AMOUNT_EXCLUDE = ['残高', '手数料', '税']

SENDER_KEYWORDS = ['摘要', '振込人', '振込依頼人', '依頼人名', '内容', 'コメント',
                    '取引内容', '備考', 'メモ', 'sender', 'summary', 'description']

DEPOSIT_KEYWORDS = ['入出金区分', '取引区分', '入出金']
DEPOSIT_KEYWORDS_EXCLUDE = ['レコード区分']
DEPOSIT_VALUES = ['入金', '振込入金', '入']


# ---------------------------------------------------------------------------
# HeuristicMapper: pattern-based column detection
# ---------------------------------------------------------------------------

class HeuristicMapper:
    """Detect date/amount/sender columns from CSV headers using keyword matching."""

    @staticmethod
    def suggest_mapping(df: pd.DataFrame) -> dict:
        """
        Analyze DataFrame columns and return a mapping dict:
        {
            'date': str or None,          # single date column
            'date_parts': {'year':..,'month':..,'day':..} or None,  # split columns
            'amount': str or None,
            'sender': str or None,
            'deposit_filter': str or None, # column to filter deposits only
            'confidence': float,           # 0.0 - 1.0
        }
        """
        cols = df.columns.tolist()
        cols_lower = [str(c).lower().strip() for c in cols]
        mapping: dict = {
            'date': None,
            'date_parts': None,
            'amount': None,
            'sender': None,
            'deposit_filter': None,
            'confidence': 0.0,
        }
        score = 0
        max_score = 3  # date + amount + sender

        # --- 1. Detect DATE ---
        # --- 1. Detect DATE ---
        # Check for split year/month/day columns first
        
        # 1-A. Try specific known patterns (Resona etc.) - relaxed match
        # We need to find columns that distinguish Year/Month/Day.
        # Strict match "年" fails for "取扱日付　年".
        
        def _find_component(keywords, must_contain=None, exclude_cols=None):
            # Helper to find a column matching a keyword, optionally requiring another substring
            # and strictly NOT in exclude_cols
            if exclude_cols is None: exclude_cols = []
            
            for i, cl in enumerate(cols_lower):
                col_original_name = cols[i]
                if col_original_name in exclude_cols:
                    continue
                    
                for kw in keywords:
                     if kw in cl:
                         if must_contain and must_contain not in cl:
                             continue
                         return cols[i]
            return None

        # Try to find Year/Month/Day columns
        # First try exact/short matches
        # Note: strict exact=True logic is already quite safe, but let's be systematic
        year_col = _find_col(cols, cols_lower, DATE_COMPONENT_YEAR, exact=True)
        month_col = _find_col(cols, cols_lower, DATE_COMPONENT_MONTH, exact=True)
        day_col = _find_col(cols, cols_lower, DATE_COMPONENT_DAY, exact=True)
        
        # If strict failed, try finding "日付...年" pattern (Resona style)
        if not (year_col and month_col and day_col):
             # Look for col containing "年" AND "日付" (e.g. "取扱日付　年")
             year_col = _find_component(['年', 'year'], must_contain='日付') or _find_component(['年', 'year'], must_contain='date')
             
             # Exclude found year_col
             exclude_for_month = [year_col] if year_col else []
             month_col = _find_component(['月', 'month'], must_contain='日付', exclude_cols=exclude_for_month) or \
                         _find_component(['月', 'month'], must_contain='date', exclude_cols=exclude_for_month)
             
             # Exclude found year/month cols
             exclude_for_day = [c for c in [year_col, month_col] if c]
             
             # Special handling for DAY: "日付" contains "日", so strict check needed.
             # If keyword is "日", it matches "日付" part. 
             # We should look for "日" that is NOT part of "日付" if possible, or just rely on exclusion.
             # Since Year/Month are already excluded, "取扱日付　日" should be the one left containing "日".
             day_col = _find_component(['日', 'day'], must_contain='日付', exclude_cols=exclude_for_day) or \
                       _find_component(['日', 'day'], must_contain='date', exclude_cols=exclude_for_day)

        if year_col and month_col and day_col:
            mapping['date_parts'] = {'year': year_col, 'month': month_col, 'day': day_col}
            score += 1
        else:
            # Look for a single date column
            date_col = _find_col(cols, cols_lower, DATE_KEYWORDS)
            if not date_col:
                # Fallback: find column with date-like values
                date_col = _detect_date_column(df)
            if date_col:
                mapping['date'] = date_col
                score += 1

        # --- 2. Detect AMOUNT ---
        amount_col = _find_col(cols, cols_lower, AMOUNT_KEYWORDS, exclude=AMOUNT_EXCLUDE)
        if not amount_col:
            amount_col = _detect_numeric_column(df, exclude_keywords=AMOUNT_EXCLUDE)
        if amount_col:
            mapping['amount'] = amount_col
            score += 1

        # --- 3. Detect SENDER ---
        sender_col = _find_col(cols, cols_lower, SENDER_KEYWORDS)
        if sender_col:
            mapping['sender'] = sender_col
            score += 1

        # --- 4. Detect DEPOSIT FILTER (optional) ---
        dep_col = _find_col(cols, cols_lower, DEPOSIT_KEYWORDS, exclude=DEPOSIT_KEYWORDS_EXCLUDE)
        if dep_col:
            # Validate: column must actually contain deposit-like values
            unique_vals = df[dep_col].dropna().astype(str).str.strip().unique()
            if any(v in DEPOSIT_VALUES for v in unique_vals):
                mapping['deposit_filter'] = dep_col

        # --- 5. EMERGENCY FALLBACK: Hardcoded O/P/Q columns (Index 14, 15, 16) ---
        # If no date detected yet, and we have enough columns, assume Resona format
        if not mapping['date'] and not mapping['date_parts']:
            if len(cols) >= 17:
                # O=14, P=15, Q=16 (0-indexed)
                # Check if they look numeric-ish just to be safe? 
                # User instruction is "Hardcoded fallback", so we trust the structure.
                # But let's verify headers vaguely match expectation or just do it?
                # User said "Header heuristic failed", so blindly trust indices if headers fail.
                
                # Check if these columns exist and assign them
                col_y = cols[14]
                col_m = cols[15]
                col_d = cols[16]
                
                mapping['date_parts'] = {'year': col_y, 'month': col_m, 'day': col_d}
                mapping['confidence'] = 0.9 # High confidence because it's a specific fallback
                
                # Also try to find Amount/Sender if missing
                if not mapping['amount']:
                     # Resona Amount usually around column 11 (L) or 12 (M)? 
                     # Let's trust existing detection for amount for now, or use a heuristic if needed.
                     # User only complained about DATE.
                     pass

        mapping['confidence'] = score / max_score
        return mapping

    @staticmethod
    def normalize_bank_data(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
        """
        Normalize a bank CSV into a standard format:
        columns: Date, Amount, Summary
        Filters to deposits only if deposit_filter is set.
        """
        result = df.copy()

        # Filter: remove summary/aggregate rows (e.g. レコード区分=合計)
        for col in result.columns:
            if 'レコード区分' in str(col):
                result = result[result[col].astype(str).str.strip() != '合計'].copy()
                break

        # Filter deposits only
        if mapping.get('deposit_filter'):
            dep_col = mapping['deposit_filter']
            if dep_col in result.columns:
                mask = result[dep_col].astype(str).str.strip().isin(DEPOSIT_VALUES)
                result = result[mask].copy()
        elif mapping.get('sender'):
            # Fallback: if no deposit_filter column, use sender/summary column
            # Keep only rows starting with "振込" (bank transfer deposits)
            sender_col = mapping['sender']
            if sender_col in result.columns:
                summary_vals = result[sender_col].astype(str).str.strip()
                mask = summary_vals.str.startswith('振込')
                result = result[mask].copy()

        # Build Date column
        # EMERGENCY OVERRIDE for Resona (Index 14, 15, 16)
        # Even if mapping says otherwise (e.g. bad template), if we have 17+ cols, use these.
        cols = result.columns.tolist()
        if len(cols) >= 17:
             try:
                 # Assume 14=Year, 15=Month, 16=Day
                 c_y = cols[14]
                 c_m = cols[15]
                 c_d = cols[16]
                 
                 # Check if they really look like dates?
                 # User said "Force", so we just do it.
                 # Ensure they are not empty?
                 # Handle "2025.0" (float strings)
                 def _safe_int_str(series):
                     return pd.to_numeric(series, errors='coerce').fillna(0).astype(int).astype(str)

                 result['Date'] = pd.to_datetime(
                    _safe_int_str(result[c_y]) + '-' +
                    _safe_int_str(result[c_m]).str.zfill(2) + '-' +
                    _safe_int_str(result[c_d]).str.zfill(2),
                    format='%Y-%m-%d', errors='coerce'
                 )
                 print("DEBUG: Force-used O/P/Q columns for Date normalization")
             except Exception as e:
                 print(f"DEBUG: Failed to force O/P/Q: {e}")
                 # Fallback to mapping
                 if mapping.get('date_parts'):
                    parts = mapping['date_parts']
                    result['Date'] = pd.to_datetime(
                        result[parts['year']].astype(int).astype(str) + '-' +
                        result[parts['month']].astype(int).astype(str).str.zfill(2) + '-' +
                        result[parts['day']].astype(int).astype(str).str.zfill(2),
                        format='%Y-%m-%d', errors='coerce'
                    )
                 elif mapping.get('date'):
                    result['Date'] = pd.to_datetime(result[mapping['date']], errors='coerce')
                 else:
                    raise ValueError("日付列が特定できません。手動でマッピングしてください。")
        elif mapping.get('date_parts'):
            parts = mapping['date_parts']
            result['Date'] = pd.to_datetime(
                result[parts['year']].astype(int).astype(str) + '-' +
                result[parts['month']].astype(int).astype(str).str.zfill(2) + '-' +
                result[parts['day']].astype(int).astype(str).str.zfill(2),
                format='%Y-%m-%d', errors='coerce'
            )
        elif mapping.get('date'):
            result['Date'] = pd.to_datetime(result[mapping['date']], errors='coerce')
        else:
            raise ValueError("日付列が特定できません。手動でマッピングしてください。")

        # Build Amount column
        if mapping.get('amount'):
            amt = result[mapping['amount']].astype(str).str.replace(',', '', regex=False)
            result['Amount'] = pd.to_numeric(amt, errors='coerce')
        else:
            raise ValueError("金額列が特定できません。手動でマッピングしてください。")

        # Build Summary column
        if mapping.get('sender'):
            result['Summary'] = result[mapping['sender']].astype(str).str.strip()
        else:
            result['Summary'] = ''

        # Drop rows with missing essential data
        result = result.dropna(subset=['Date', 'Amount'])
        result = result[result['Amount'] > 0]

        return result[['Date', 'Amount', 'Summary']].reset_index(drop=True)


# ---------------------------------------------------------------------------
# TemplateManager: Supabase-backed template storage
# ---------------------------------------------------------------------------

class TemplateManager:
    """Persist column mappings in Supabase csv_templates table.

    Supports:
      - User-specific templates (user_id set)
      - Shared/global templates (user_id=None, shared=True)
    """

    @staticmethod
    def get_header_hash(columns: list) -> str:
        """SHA256 hash of column names."""
        key = '|'.join(str(c).strip() for c in columns)
        return hashlib.sha256(key.encode('utf-8')).hexdigest()[:16]

    @staticmethod
    def lookup(db_client, columns: list, user_id=None):
        """Return saved mapping for this header layout, or None.

        Args:
            db_client: DBClient instance
            columns: list of column names from the CSV
            user_id: optional user ID for multi-tenant filtering
        """
        header_hash = TemplateManager.get_header_hash(columns)
        try:
            result = db_client.lookup_csv_template(header_hash, user_id=user_id)
            if result:
                # Parse mapping from JSON if needed
                mapping = result.get('mapping')
                if isinstance(mapping, str):
                    mapping = json.loads(mapping)
                return {
                    'mapping': mapping,
                    'label': result.get('label', ''),
                    'columns': result.get('columns'),
                    'shared': result.get('shared', False),
                }
            return None
        except Exception:
            return None

    @staticmethod
    def save_template(db_client, columns: list, mapping: dict,
                      label: str = '', user_id=None, shared: bool = False):
        """Save a confirmed mapping for future reuse.

        Args:
            db_client: DBClient instance
            columns: list of column names
            mapping: detected/confirmed column mapping dict
            label: human-readable label (e.g. 'りそな銀行')
            user_id: owner user ID (None = global)
            shared: if True, template is visible to all users
        """
        header_hash = TemplateManager.get_header_hash(columns)
        # Remove confidence from stored mapping (it's computed dynamically)
        store_mapping = {k: v for k, v in mapping.items() if k != 'confidence'}
        db_client.upsert_csv_template(
            header_hash=header_hash,
            mapping=store_mapping,
            columns=[str(c) for c in columns],
            label=label,
            user_id=user_id,
            shared=shared,
        )

    @staticmethod
    def delete_template(db_client, columns: list, user_id=None):
        """Delete a template by header hash."""
        header_hash = TemplateManager.get_header_hash(columns)
        db_client.delete_csv_template(header_hash, user_id=user_id)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _find_col(cols, cols_lower, keywords, exclude=None, exact=False):
    """Find first column matching any keyword."""
    for i, cl in enumerate(cols_lower):
        if exclude and any(ex in cl for ex in [e.lower() for e in exclude]):
            continue
        for kw in keywords:
            kw_l = kw.lower()
            if exact:
                if cl == kw_l:
                    return cols[i]
            else:
                if kw_l in cl:
                    return cols[i]
    return None


def _detect_date_column(df: pd.DataFrame):
    """Find column with date-like string values (e.g. 2025/01/15)."""
    date_pattern = re.compile(r'\d{4}[/\-]\d{1,2}[/\-]\d{1,2}')
    for col in df.columns:
        sample = df[col].dropna().head(10).astype(str)
        matches = sample.apply(lambda x: bool(date_pattern.match(x)))
        if matches.sum() >= 3:
            return col
    return None


def _detect_numeric_column(df: pd.DataFrame, exclude_keywords=None):
    """Find the first numeric-looking column not in exclude list."""
    exclude_keywords = [e.lower() for e in (exclude_keywords or [])]
    for col in df.columns:
        if any(ex in str(col).lower() for ex in exclude_keywords):
            continue
        sample = df[col].dropna().head(10)
        try:
            nums = pd.to_numeric(sample.astype(str).str.replace(',', ''), errors='coerce')
            if nums.notna().sum() >= 3:
                return col
        except Exception:
            continue
    return None
