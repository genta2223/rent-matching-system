import pandas as pd
from db_client import DBClient

if __name__ == "__main__":
    db = DBClient()
    tenants = db.fetch_tenants()
    target = None
    
    for _, row in tenants.iterrows():
        if str(row['PropertyID']) == '9':
            target = row.to_dict()
            break
            
    if target:
        values = target.get('Values', {})
        if not isinstance(values, dict):
            values = {}
        values['BillingZip'] = '661-0971'
        values['BillingAddress'] = '尼崎市瓦宮2-5-23'
        values['BillingName'] = '中井則子'
        
        target['Values'] = values
        db.update_tenant('9', target)
        print("Successfully updated Tenant 9")
    else:
        print("Tenant 9 not found")
