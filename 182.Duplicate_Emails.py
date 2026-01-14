# Input:
# Person table:
# +----+---------+
# | id | email   |
# +----+---------+
# | 1  | a@b.com |
# | 2  | c@d.com |
# | 3  | a@b.com |
# +----+---------+
# Output:
# +---------+
# | Email   |
# +---------+
# | a@b.com |
# +---------+
# Explanation: a@b.com is repeated two times.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example").getOrCreate()


data = [(1, "a@b.com"), (2, "c@d.com"), (3, "a@b.com")]

# Columns
columns = ["id", "email"]
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

from pyspark.sql.functions import *

dup_df=df.groupby("email").agg(count("*").alias("cnt")).filter(col("cnt")>1).select("email")

dup_df.show()


print("Using SQL")

df.createTempView("tmp")

spark.sql("select email from tmp group by email having count(*) >1").show()