import pandas as pd
import os

df = pd.read_csv('private_data/rent_roll.csv')
memo_fix = "2026/01/15 60000円入金 2025年12月分全額充当 2026年1月分のうち26500円入金 2025/12/16 60000円入金 2025年11月分全額充当 2025年12月分のうち24500円入金"
df.loc[df['PropertyID'] == 3, 'LatestPaymentMemo'] = memo_fix
df.to_csv('private_data/rent_roll.csv', index=False, encoding='utf-8-sig')
print("Fixed memo for Property ID 3.")
