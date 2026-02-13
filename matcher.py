import pandas as pd
import os
import sys

def normalize_name(name):
    """Normalize names for matching (remove spaces, convert to upper, strip common prefixes)"""
    if not isinstance(name, str) or name.lower() == 'nan':
        return ""
    # Remove whitespace
    name = name.replace(" ", "").replace("　", "")
    # Remove common bank prefixes
    for prefix in ["振込　", "振込", "ﾂｲｶ", "ｻｲｿｳ"]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    return name.upper()

class TenantRecord:
    def __init__(self, room_no, name, rent, candidates):
        self.room_no = room_no
        self.name = name
        self.rent = rent
        self.candidates = candidates
        self.paid_amount = 0
        self.status = "Unpaid"
        self.details = []

    def to_dict(self):
        return {
            'RoomNo': self.room_no,
            'TenantName': self.name,
            'ExpectedRent': self.rent,
            'PaidAmount': self.paid_amount,
            'Status': self.status,
            'MatchDetails': "; ".join(self.details)
        }

def run_matching(bank_csv_path, rent_roll_path, output_path):
    print(f"Loading bank data from {bank_csv_path}...")
    try:
        try:
            bank_df = pd.read_csv(bank_csv_path, encoding='cp932')
        except UnicodeDecodeError:
            bank_df = pd.read_csv(bank_csv_path, encoding='utf-8-sig')
        bank_df['used'] = False
    except Exception as e:
        print(f"Error loading bank CSV: {e}")
        return

    print(f"Loading rent roll from {rent_roll_path}...")
    try:
        rent_df = pd.read_csv(rent_roll_path)
    except Exception as e:
        print(f"Error loading rent roll: {e}")
        return

    tenants = []
    for _, row in rent_df.iterrows():
        candidates = [normalize_name(row.get(f'BankMatchName{i}')) for i in range(1, 4)]
        candidates = [c for c in candidates if c]
        tenants.append(TenantRecord(row['RoomNo'], row['TenantName'], row['MonthlyRent'], candidates))

    # --- Step 1: Individual Exact Matching ---
    for tenant in tenants:
        for cand in tenant.candidates:
            for idx, tx in bank_df[~bank_df['used']].iterrows():
                summary = normalize_name(str(tx['摘要']))
                if cand in summary and tx['金額'] == tenant.rent:
                    tenant.paid_amount = tx['金額']
                    tenant.status = "Paid"
                    tenant.details.append(f"Exact match: {tx['摘要']}")
                    bank_df.at[idx, 'used'] = True
                    break
            if tenant.status == "Paid": break

    # --- Step 2: Batch/Guarantee Matching ---
    unpaid_tenants = [t for t in tenants if t.status == "Unpaid"]
    all_cand_names = set()
    for t in unpaid_tenants:
        for c in t.candidates: all_cand_names.add(c)

    for name in all_cand_names:
        matching_txs = bank_df[(~bank_df['used']) & (bank_df['摘要'].fillna('').apply(normalize_name).str.contains(name, na=False))]
        if matching_txs.empty: continue
        
        sharing_tenants = [t for t in unpaid_tenants if name in t.candidates]
        total_tx = matching_txs['金額'].sum()
        total_rent = sum(t.rent for t in sharing_tenants)

        if total_tx == total_rent:
            for t in sharing_tenants:
                t.status = "Paid"
                t.paid_amount = t.rent
                t.details.append(f"Batch match via '{name}'")
            for idx in matching_txs.index: bank_df.at[idx, 'used'] = True
        else:
            # Try matching individual transactions within the shared name group
            for idx, tx in matching_txs.iterrows():
                for t in sharing_tenants:
                    if t.status == "Unpaid" and t.rent == tx['金額']:
                        t.status = "Paid"
                        t.paid_amount = tx['金額']
                        t.details.append(f"Match in batch: {tx['摘要']}")
                        bank_df.at[idx, 'used'] = True
                        break

    # Save results
    results_df = pd.DataFrame([t.to_dict() for t in tenants])
    results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Report generated: {output_path}")
    print(results_df[['RoomNo', 'TenantName', 'Status', 'PaidAmount']])

if __name__ == "__main__":
    # Move to the private_data directory for sensitive files
    data_dir = "private_data"
    
    # Dynamically find the bank CSV in the private_data folder
    bank_csv = None
    if os.path.exists(data_dir):
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv') and f != 'rent_roll.csv' and not f.startswith('reconciliation')]
        if csv_files:
            # Pick the most recent one if multiple exist
            bank_csv = os.path.join(data_dir, sorted(csv_files, reverse=True)[0])
    
    rent_roll = os.path.join(data_dir, "rent_roll.csv")
    output = os.path.join(data_dir, "reconciliation_report.csv")
    
    if bank_csv and os.path.exists(rent_roll):
        run_matching(bank_csv, rent_roll, output)
    else:
        print("Required files missing in 'private_data' folder.")
        if not bank_csv: print("No bank CSV found in private_data/")
        if not os.path.exists(rent_roll): print("Missing: private_data/rent_roll.csv")
