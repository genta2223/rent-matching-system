"""
csv_ai_mapper.py — Heuristic-based bank CSV auto-detection + template management.

No LLM required. Uses pattern matching on header names and data types.
Templates are stored as JSON keyed by header hash for instant re-use.
"""

import hashlib
import json
import os
import re
from datetime import datetime
from pathlib import Path

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
        mapping = {
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
        # Check for split year/month/day columns first
        year_col = _find_col(cols, cols_lower, DATE_COMPONENT_YEAR, exact=True)
        month_col = _find_col(cols, cols_lower, DATE_COMPONENT_MONTH, exact=True)
        day_col = _find_col(cols, cols_lower, DATE_COMPONENT_DAY, exact=True)

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
# TemplateManager: hash-based template storage
# ---------------------------------------------------------------------------

TEMPLATE_FILE = os.path.join(os.path.dirname(__file__), '.csv_templates.json')


class TemplateManager:
    """Persist column mappings keyed by CSV header hash."""

    @staticmethod
    def get_header_hash(columns: list) -> str:
        """SHA256 hash of sorted column names."""
        key = '|'.join(str(c).strip() for c in columns)
        return hashlib.sha256(key.encode('utf-8')).hexdigest()[:16]

    @staticmethod
    def _load() -> dict:
        if os.path.exists(TEMPLATE_FILE):
            with open(TEMPLATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    @staticmethod
    def _save(data: dict):
        with open(TEMPLATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    @classmethod
    def lookup(cls, columns: list) -> dict | None:
        """Return saved mapping for this header layout, or None."""
        h = cls.get_header_hash(columns)
        store = cls._load()
        return store.get(h)

    @classmethod
    def save_template(cls, columns: list, mapping: dict, label: str = ''):
        """Save a confirmed mapping for future reuse."""
        h = cls.get_header_hash(columns)
        store = cls._load()
        entry = {
            'mapping': mapping,
            'label': label,
            'columns': [str(c) for c in columns],
            'saved_at': datetime.now().isoformat(),
        }
        store[h] = entry
        cls._save(store)

    @classmethod
    def delete_template(cls, columns: list):
        h = cls.get_header_hash(columns)
        store = cls._load()
        if h in store:
            del store[h]
            cls._save(store)


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


def _detect_date_column(df: pd.DataFrame) -> str | None:
    """Find column with date-like string values (e.g. 2025/01/15)."""
    date_pattern = re.compile(r'\d{4}[/\-]\d{1,2}[/\-]\d{1,2}')
    for col in df.columns:
        sample = df[col].dropna().head(10).astype(str)
        matches = sample.apply(lambda x: bool(date_pattern.match(x)))
        if matches.sum() >= 3:
            return col
    return None


def _detect_numeric_column(df: pd.DataFrame, exclude_keywords=None) -> str | None:
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
