import pandas as pd
import random
from datetime import datetime, timedelta
import os

brands = [
    "Milo", "KitKat", "Maggi", "Nescafé", "Cerelac",
    "Nestum", "Lactogrow", "Koko Krunch", "Gold Cornflakes"
]
executives = ["Amira", "Hazim", "John", "Wei Yong", "Viknesh"]

rows = []
start_date = datetime(2022, 6, 24)

for i in range(10000):
    date = start_date + timedelta(days=random.randint(0, 30))
    item_code = f"NES{i+1:03d}"
    brand = random.choice(brands)
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(30, 150), 2)
    executive = random.choice(executives)
    rows.append([date.strftime("%Y-%m-%d"), item_code, brand, quantity, unit_price, executive])

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
