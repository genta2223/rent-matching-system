
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from matcher import TenantRecord

def debug_tenant(property_id):
    rent_roll = pd.read_csv('private_data/rent_roll.csv', encoding='utf-8-sig')
    ledger = pd.read_csv('private_data/payment_ledger.csv', encoding='utf-8-sig')
    
    target_row = rent_roll[rent_roll['PropertyID'] == property_id].iloc[0]
    t = TenantRecord(target_row)
    
    room_payments = ledger[ledger['PropertyID'].astype(str) == str(property_id)]
    for _, p in room_payments.iterrows():
        t.ledger_payments.append({'Date': pd.to_datetime(p['PaymentDate']), 'Amount': p['Amount'], 'Allocations': [], 'Surplus': 0})

    today = datetime(2026, 2, 13)
    t.calculate_debts(today)
    t.allocate_payments()
    
    print(f"\n--- Property {property_id} Debug ---")
    print(f"Memo: {t.delinquency_memo}")
    print(f"Anchor Date: {t.memo_anchor_date}")
    print(f"Monthly Rent: {t.rent}")
    print("\nDebts:")
    for d in t.debts:
        print(f"  {d['month'].strftime('%Y-%m')}: Amount {d['amount']}, Paid {d['paid']}, Balance {d['amount'] - d['paid']}")
    
    print("\nLedger Payments & Allocations:")
    for lp in t.ledger_payments:
        print(f"  {lp['Date'].strftime('%Y-%m-%d')} Amount {lp['Amount']}")
        for a in lp['Allocations']:
            print(f"    -> {a['Month'].strftime('%Y-%m')} Amount {a['Amount']} {'(Full)' if a['IsFull'] else ''}")
        if lp['Surplus'] > 0:
            print(f"    -> Surplus: {lp['Surplus']}")

debug_tenant(3)
debug_tenant(14)
debug_tenant(9)
