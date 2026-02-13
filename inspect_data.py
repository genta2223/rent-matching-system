import pandas as pd
import os

df = pd.read_csv('private_data/rent_roll.csv')
print("Columns:", df.columns.tolist())
id3 = df[df['PropertyID'] == 3]
print("\nProperty ID 3 Data:")
print(id3.iloc[0].to_dict())
