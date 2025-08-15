import pandas as pd
import random

# Define brand-product mappings
brand_products = {
    "Milo": ["Milo Activ-Go 3-in-1", "Milo UHT 200ml", "Milo Powder 1kg"],
    "KitKat": ["KitKat 4-Finger Bar", "KitKat Chunky", "KitKat Mini"],
    "Maggi": ["Maggi Curry Noodles", "Maggi Tom Yam", "Maggi Chicken Stock"],
    "Nescafé": ["Nescafé Classic 100g", "Nescafé Latte", "Nescafé Gold Blend"],
    "Cerelac": ["Cerelac Wheat & Milk", "Cerelac Rice & Milk", "Cerelac Banana"],
    "Nestum": ["Nestum Original 250g", "Nestum Honey", "Nestum Chocolate"],
    "Lactogrow": ["Lactogrow 3 Milk Powder", "Lactogrow 4 Milk Powder"],
    "Koko Krunch": ["Koko Krunch Chocolate", "Koko Krunch Duo"],
    "Gold Cornflakes": ["Gold Cornflakes 275g", "Gold Cornflakes Honey"]
}

categories = {
    "Milo": "Beverage",
    "KitKat": "Snack",
    "Maggi": "Instant Noodles",
    "Nescafé": "Beverage",
    "Cerelac": "Baby Food",
    "Nestum": "Cereal",
    "Lactogrow": "Baby Formula",
    "Koko Krunch": "Cereal",
    "Gold Cornflakes": "Cereal"
}

rows = []

for i in range(100):
    item_code = f"NES{i+1:03d}"
    brand = random.choice(list(brand_products.keys()))
    item_name = random.choice(brand_products[brand])
    category = categories[brand]
    master_unit_price = round(random.uniform(30, 150), 2)
    rows.append([item_code, item_name, category, brand, master_unit_price])

df = pd.DataFrame(rows, columns=["item_code", "item_name", "category", "brand", "master_unit_price"])

# Save CSV
df.to_csv("master_data.csv", index=False)