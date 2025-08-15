import pandas as pd
import random
from datetime import datetime, timedelta
import os

# Load fixed product master
product_master = pd.read_csv("dags\db_config\master_data.csv")

executives = ["Amira", "Hazim", "John", "Wei Yong", "Viknesh"]

rows = []
start_date = datetime(2022, 1, 1)
end_date = datetime(2025, 6, 30)

# Calculate total days between start and end
delta_days = (end_date - start_date).days

for i in range(100):
    date = start_date + timedelta(days=random.randint(0, delta_days))
    product = product_master.sample(1).iloc[0]
    quantity = random.randint(1, 10)
    executive = random.choice(executives)
    rows.append([date.strftime("%Y-%m-%d"),
        product["item_code"],
        product["brand"],
        quantity,
        product["master_unit_price"],
        executive])

df = pd.DataFrame(rows, columns=["date", "item_code", "brand_name", "quantity", "unit_price", "executive"])
print(df)

timestamp = datetime.now().strftime("%d%m%Y_%I%M_%p")

filename = f"sales_{timestamp}.csv"

# Get the folder path of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Build full path
filename = f"sales_{timestamp}.csv"
full_path = os.path.join(script_dir, filename)

# Save CSV
df.to_csv(full_path, index=False)
