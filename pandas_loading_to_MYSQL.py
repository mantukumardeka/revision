import pandas as pd
from sqlalchemy import create_engine

# =========================
# STEP 1: FILE PATH
# =========================
file_path = "/Users/mantukumardeka/Downloads/orders.csv"   # already available

# =========================
# STEP 2: READ CSV
# =========================
df = pd.read_csv(
    file_path,
    na_values=['Not Available', 'unknown']
)

# =========================
# STEP 3: CLEAN COLUMN NAMES
# =========================
df.columns = df.columns.str.lower().str.replace(' ', '_')

# =========================
# STEP 4: TRANSFORM (SAFE)
# =========================
# Only create if columns exist
if 'list_price' in df.columns and 'discount_percent' in df.columns:
    df['discount'] = df['list_price'] * df['discount_percent'] * 0.01

if 'list_price' in df.columns and 'discount' in df.columns:
    df['sale_price'] = df['list_price'] - df['discount']

if 'sale_price' in df.columns and 'cost_price' in df.columns:
    df['profit'] = df['sale_price'] - df['cost_price']

# Convert date safely
if 'order_date' in df.columns:
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

# Drop unwanted columns if present
cols_to_drop = ['list_price', 'cost_price', 'discount_percent']
df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

# =========================
# STEP 5: LOAD TO MYSQL
# =========================
# 👉 Update your MySQL password
username = "root"
password = "Hadoop@123".replace("@", "%40")

engine = create_engine(f"mysql+pymysql://{username}:{password}@localhost:3306/mkd")

df.to_sql(
    name='orders',
    con=engine,
    index=False,
    if_exists='replace'   # use 'append' for incremental load
)

print("✅ Data successfully loaded into MySQL table: mkd.orders")