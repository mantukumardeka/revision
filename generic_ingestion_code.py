import os
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
# DETECT DELIMITER (CSV / TAB)
# =========================
def detect_delimiter(file_path):
    with open(file_path, 'r') as f:
        first_line = f.readline()
        if '\t' in first_line:
            return '\t'
        else:
            return ','

# =========================
# READ CSV / TXT ONLY
# =========================
def read_file(file_path):
    sep = detect_delimiter(file_path)

    df = pd.read_csv(
        file_path,
        dtype=str,          # ✅ preserve exact data
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
# CLEAN DATA (SAFE)
# =========================
def clean_data(df):
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

        # 🔥 Fix trailing comma (TXN115 issue)
        df[col] = df[col].str.replace(r',$', '', regex=True)

        # Convert empty string → NULL
        df[col] = df[col].replace('', None)

    return df

# =========================
# DEFINE MYSQL SCHEMA
# =========================
def define_mysql_schema(df):
    dtype_dict = {}

    for col in df.columns:
        col_lower = col.lower()

        # ✅ Only convert date columns
        if any(k in col_lower for k in ['date', 'time']):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                dtype_dict[col] = types.DateTime()
            except:
                dtype_dict[col] = types.String(255)

        # ✅ Everything else → STRING (safe)
        else:
            max_len = df[col].astype(str).str.len().max()

            if max_len < 255:
                dtype_dict[col] = types.String(255)
            else:
                dtype_dict[col] = types.Text()

    return df, dtype_dict

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

        # ✅ Only process CSV / TXT
        if not (file.endswith(".csv") or file.endswith(".txt")):
            print(f"⏭ Skipping (not CSV/TXT): {file}")
            continue

        file_path = os.path.join(FOLDER_PATH, file)

        print(f"\n🚀 Processing: {file}")

        df = read_file(file_path)

        df = clean_columns(df)
        df = clean_data(df)

        # 🔥 Safe schema (no data loss)
        df, dtype_dict = define_mysql_schema(df)

        table_name = file.split('.')[0].lower()

        try:
            df.to_sql(
                name=table_name,
                con=engine,
                index=False,
                if_exists='replace',
                dtype=dtype_dict
            )

            print(f"✅ Loaded successfully: {table_name}")

        except Exception as e:
            print(f"❌ Error loading {file}: {e}")

# =========================
# RUN
# =========================
if __name__ == "__main__":
    process_files()