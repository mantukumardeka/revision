from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

conf = SparkConf().setMaster("local[*]").setAppName("MaskingExample")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# ------------------------
# EMAIL MASKING UDF
# m******t@gmail.com
# ------------------------
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

# ------------------------
# MOBILE MASKING UDF
# 9*****434
# ------------------------
def mask_mobile(mobile):
    try:
        first = mobile[0]                 # first digit
        last = mobile[-1]                 # last digit
        stars = "*" * (len(mobile) - 2)   # stars in between
        return first + stars + last
    except:
        return mobile

mask_mobile_udf = udf(mask_mobile, StringType())

# ------------------------
# CREATE DATAFRAME
# ------------------------
df = spark.createDataFrame([
    ("Renuka1992@gmail.com", "9856765434"),
    ("anbu.arasu@gmail.com", "9844567788"),
    ("mantuct@gmail.com", "9876543210")
], ["email", "mobile"])

df.show(truncate=False)

# ------------------------
# APPLY MASKING UDFs
# ------------------------

maskeddf = df.withColumn("email", mask_email_udf("email")) \
             .withColumn("mobile", mask_mobile_udf("mobile"))

maskeddf.show(truncate=False)


#
