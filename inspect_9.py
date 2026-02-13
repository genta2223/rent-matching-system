import pandas as pd
df = pd.read_csv('private_data/rent_roll.csv')
id9 = df[df['PropertyID'] == 9]
print("Property ID 9 Data:")
print(id9.iloc[0].to_dict())
