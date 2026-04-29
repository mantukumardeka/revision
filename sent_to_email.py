import pandas as pd
from sqlalchemy import create_engine
import smtplib
from email.message import EmailMessage

# =========================
# DB CONFIG
# =========================
DB_NAME = "mkd"
USERNAME = "root"
PASSWORD = "Hadoop@123".replace("@", "%40")

engine = create_engine(
    f"mysql+pymysql://{USERNAME}:{PASSWORD}@localhost:3306/{DB_NAME}"
)

# =========================
# EMAIL CONFIG
# =========================
SENDER_EMAIL = "mantuctl@gmail.com"
APP_PASSWORD = "your_16_digit_app_password"
RECEIVER_EMAIL = "mantuctl@gmail.com"

# =========================
# GET TABLES
# =========================
def get_tables():
    df = pd.read_sql("SHOW TABLES", engine)
    return df.iloc[:, 0].tolist()

# =========================
# NULL CHECK
# =========================
def get_null_report(table):
    df = pd.read_sql(f"SELECT * FROM `{table}`", engine)

    result = {
        "table_name": table,
        "total_rows": len(df)
    }

    for col in df.columns:
        result[f"{col}_nulls"] = df[col].isnull().sum()

    return result

# =========================
# SEND EMAIL
# =========================
def send_email(file_path):

    msg = EmailMessage()
    msg['Subject'] = "📊 NULL Data Quality Report"
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL

    msg.set_content("Hi,\n\nPlease find attached NULL report.\n\nThanks")

    # Attach file
    with open(file_path, 'rb') as f:
        file_data = f.read()

    msg.add_attachment(
        file_data,
        maintype='application',
        subtype='octet-stream',
        filename=file_path
    )

    # Send mail
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(SENDER_EMAIL, APP_PASSWORD)
        smtp.send_message(msg)

    print("📧 Email sent successfully!")

# =========================
# MAIN FUNCTION
# =========================
def run():

    print("🚀 Starting Data Quality Check...\n")

    tables = get_tables()
    print(f"📂 Found {len(tables)} tables\n")

    results = []

    for table in tables:
        print(f"🔍 Checking: {table}")
        try:
            res = get_null_report(table)
            results.append(res)
        except Exception as e:
            print(f"❌ Error in {table}: {e}")

    df = pd.DataFrame(results)

    # Fill missing columns
    df = df.fillna(0)

    # Save CSV
    file_path = "null_report.csv"
    df.to_csv(file_path, index=False)

    print("\n✅ NULL Report Generated!")

    # Send Email
    send_email(file_path)

    print("\n🎉 Process Completed Successfully!")

# =========================
# RUN
# =========================
if __name__ == "__main__":
    run()