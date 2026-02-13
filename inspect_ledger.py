import pandas as pd
ledger_df = pd.read_csv('private_data/payment_ledger.csv')
id3_ledger = ledger_df[ledger_df['PropertyID'] == 3]
print("Ledger entries for Property 3:")
print(id3_ledger)
