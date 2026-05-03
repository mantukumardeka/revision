from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

data = [
    (1, " Alice ", "goa", None, 25, "5000", "2025-01-01", ["Python", "SQL"], "alice@gmail.com", "+91 9876543210"),
    (2, "", " mumbai ", 2000, -5, "abc", "2025/02/01", ["Java", "Python"], "user2yahoo.com", "+91 9123456780"),
    (3, None, None, None, 30, "70000", "2025-03-01", None, "test.user@outlook.com", None),                         # null allowed
    (4, "Bob@", "delhi", 150000, 40, "90000", "2025-04-01", ["Scala"], "bob@@gmail.com", "+91 9012345678"),
    (4, "Bob@", "delhi", 150000, 40, "90000", "2025-04-01", ["Scala"], "bob@@gmail.com", "+91 9012345678"),        # duplicate

    (5, "John ", " chennai", 50000, 28, "50000", "2025-05-01", ["Python"], "john.doe@company.com", "+91 9234567890"),
    (6, "Sara#", "kolkata ", None, 22, "30000", "2025-06-01", ["SQL"], "sara#mail.com", "+91 9345678901"),
    (7, "Mike", "bangalore", 70000, 35, "70000", "2025-07-01", ["Java", "Scala"], "mike123@gmail", "+91 9456789012"),
    (8, "Anna", "hyderabad", None, 27, "45000", "2025-08-01", ["Python"], "anna@.com", "+91 9567890123"),
    (9, "Tom ", " pune", 60000, 29, "60000", "2025-09-01", ["SQL", "Python"], "tom.pune@yahoo.co.in", "+91 9678901234"),

    (10, "Lucy", "noida", 80000, 31, "80000", "2025-10-01", ["Java"], "lucy@company.org", "+91 9789012345"),
    (11, "David$", "gurgaon", None, -10, "10000", "2025-11-01", ["Scala"], "david$gmail.com", "+91 9890123456"),
    (12, "Emma", " jaipur ", 90000, 26, "90000", "2025-12-01", ["Python", "SQL"], "emma@gmail..com", "+91 9901234567"),
    (13, "Chris", None, 40000, 33, "40000", "2025-01-15", None, "chris@domain", "+91 9011122233"),
    (14, "Nina ", "lucknow", None, 24, "35000", "2025-02-20", ["SQL"], " nina.lucknow@gmail.com ", "+91  9122233344 "), # spacing issue
    (15, "Raj@", "patna", 30000, 21, "30000", "2025-03-10", ["Python"], "@yahoo.com", "+91 9233344455")
]

columns = ["id", "name", "city", "salary", "age", "salary_str", "date", "skills", "email", "mobile"]

df = spark.createDataFrame(data, columns)

df.show(truncate=False)
# Masking using UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def mask_mobile(mobile):
    try:
        mobile = mobile.strip()
        first=mobile[:2] # [1]
        last=mobile[-2:] # [-1]
        stars="*" * (len(mobile)-4)
        return first+stars+last
    except:
        return mobile
df_mb=udf(mask_mobile,StringType())


def mask_email(email):
    try:
        at_index = email.index("@")
        first = email[0]              # first letter
        last = email[at_index - 1]    # letter before @
        stars = "*" * (at_index - 2)  # stars in between
        return first + stars + last + email[at_index:]
    except:
        return email

mask_email_udf = udf(mask_email, StringType())

maskeddf = df.withColumn("newemail", mask_email_udf("email"))\
             .withColumn("newmobile", df_mb("mobile"))

maskeddf.show(truncate=False)