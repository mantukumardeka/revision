import os
import re
import pandas as pd
from sqlalchemy import create_engine, types

# =========================
# CONFIG
# =========================
FOLDER_PATH = "/Users/mantukumardeka/Desktop/datafolder"
DB_NAME = "mkd"
USERNAME = "root"
PASSWORD = "Hadoop@123".replace("@", "%40")

engine = create_engine(f"mysql+pymysql://{USERNAME}:{PASSWORD}@localhost:3306/{DB_NAME}")

# =========================
# DETECT DELIMITER
# =========================
def detect_delimiter(file_path):
    with open(file_path, 'r') as f:
        first_line = f.readline()
        if '\t' in first_line:
            return '\t'
        return ','

# =========================
# READ FILE (SAFE)
# =========================
def read_file(file_path):
    sep = detect_delimiter(file_path)

    df = pd.read_csv(
        file_path,
        dtype=str,        # 🔥 preserve original data
        sep=sep,
        engine="python"
    )

    return df

# =========================
# CLEAN COLUMN NAMES
# =========================
def clean_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
    )
    return df

# =========================
# CLEAN DATA
# =========================
def clean_data(df):
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

        # 🔥 Remove trailing commas (TXN115,)
        df[col] = df[col].str.replace(r',$', '', regex=True)

        # 🔥 Remove commas in numbers (1,200 → 1200)
        df[col] = df[col].str.replace(',', '')

        # Convert empty → NULL
        df[col] = df[col].replace(['', 'nan', 'None', 'NULL'], None)

    return df

# =========================
# DEFINE MYSQL SCHEMA
# =========================
def define_mysql_schema(df):
    dtype_dict = {}

    for col in df.columns:
        col_lower = col.lower()

        # ✅ Only convert DATE columns
        if any(k in col_lower for k in ['date', 'time']):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                dtype_dict[col] = types.DateTime()
            except:
                dtype_dict[col] = types.String(255)

        else:
            # ✅ Keep everything else as STRING (safe, no data loss)
            max_len = df[col].astype(str).str.len().max()

            if max_len and max_len < 255:
                dtype_dict[col] = types.String(255)
            else:
                dtype_dict[col] = types.Text()

    return df, dtype_dict

# =========================
# CLEAN TABLE NAME
# =========================
def clean_table_name(file):
    table_name = file.split('.')[0]

    table_name = table_name.strip()
    table_name = table_name.lower()
    table_name = re.sub(r'\s+', '_', table_name)
    table_name = re.sub(r'[^a-z0-9_]', '', table_name)

    return table_name

# =========================
# MAIN PROCESS
# =========================
def process_files():

    if not os.path.exists(FOLDER_PATH):
        print("❌ Folder not found")
        return

    files = os.listdir(FOLDER_PATH)

    if not files:
        print("⚠️ No files found")
        return

    print(f"📂 Found {len(files)} files")

    for file in files:

        # Only CSV / TXT
        if not (file.endswith(".csv") or file.endswith(".txt")):
            print(f"⏭ Skipping: {file}")
            continue

        file_path = os.path.join(FOLDER_PATH, file)

        print(f"\n🚀 Processing: {file}")

        try:
            df = read_file(file_path)

            df = clean_columns(df)
            df = clean_data(df)

            df, dtype_dict = define_mysql_schema(df)

            table_name = clean_table_name(file)

            print(f"📄 Table Name: {table_name}")

            df.to_sql(
                name=table_name,
                con=engine,
                index=False,
                if_exists='replace',
                dtype=dtype_dict
            )

            print(f"✅ Loaded successfully: {table_name}")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

# =========================
# RUN
# =========================
if __name__ == "__main__":
    process_files()