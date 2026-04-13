import pandas as pd
from sqlalchemy import create_engine

# =========================
# CONFIG
# =========================
DB_NAME = "mkd"
USERNAME = "root"
PASSWORD = "Hadoop@123".replace("@", "%40")

engine = create_engine(f"mysql+pymysql://{USERNAME}:{PASSWORD}@localhost:3306/{DB_NAME}")

# =========================
# GET ALL TABLES (FIXED)
# =========================
def get_tables():
    query = "SHOW TABLES"
    df = pd.read_sql(query, engine)

    # 🔥 dynamic column name fix
    return df.iloc[:, 0].tolist()

# =========================
# GET COLUMNS FOR TABLE
# =========================
def get_columns(table):
    query = f"SHOW COLUMNS FROM `{table}`"
    df = pd.read_sql(query, engine)

    return df['Field'].tolist()

# =========================
# NULL CHECK PER TABLE
# =========================
def check_nulls(table):

    columns = get_columns(table)

    # total rows
    total_query = f"SELECT COUNT(*) as total_rows FROM `{table}`"
    total_rows = pd.read_sql(total_query, engine)['total_rows'][0]

    result = {
        "table_name": table,
        "total_rows": total_rows
    }

    # null count per column
    for col in columns:
        query = f"SELECT COUNT(*) as cnt FROM `{table}` WHERE `{col}` IS NULL"
        null_count = pd.read_sql(query, engine)['cnt'][0]

        result[f"{col}_nulls"] = null_count

    return result

# =========================
# MAIN FUNCTION
# =========================
def run_data_quality_check():

    tables = get_tables()

    print(f"📊 Found {len(tables)} tables")

    final_result = []

    for table in tables:
        print(f"🔍 Checking: {table}")

        try:
            res = check_nulls(table)
            final_result.append(res)

        except Exception as e:
            print(f"❌ Error in table {table}: {e}")

    # Convert to DataFrame
    df = pd.DataFrame(final_result)

    # Fill NaN (different columns across tables)
    df.fillna(0, inplace=True)

    print("\n✅ FINAL NULL REPORT:\n")
    print(df)

    # Save report
    df.to_csv("null_report.csv", index=False)

    print("\n📁 Report saved as: null_report.csv")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    run_data_quality_check()