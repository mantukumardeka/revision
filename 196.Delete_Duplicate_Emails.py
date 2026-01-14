#
# Input:
# Person table:
# +----+------------------+
# | id | email            |
# +----+------------------+
# | 1  | john@example.com |
# | 2  | bob@example.com  |
# | 3  | john@example.com |
# +----+------------------+
# Output:
# +----+------------------+
# | id | email            |
# +----+------------------+
# | 1  | john@example.com |
# | 2  | bob@example.com  |
# +----+------------------+
# Explanation: john@example.com is repeated two times. We keep the row with the smallest Id = 1.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Person").getOrCreate()

person_data = [
    (1, "john@example.com"),
    (2, "bob@example.com"),
    (3, "john@example.com")
]

person_cols = ["id", "email"]
person_df = spark.createDataFrame(person_data, person_cols)

person_df.show()

from pyspark.sql.functions import *
from pyspark.sql.window import Window


final_df=person_df.withColumn("rn", row_number().over(Window.partitionBy("email").orderBy(("id"))))\
    .filter(col("rn")==1).drop("rn")


final_df.show()

print("Using SQL")

person_df.createTempView("person")

spark.sql("""
SELECT id, email
FROM (
    SELECT id,
           email,
           ROW_NUMBER() OVER (PARTITION BY email ORDER BY id) AS rn
    FROM person
) t
WHERE rn = 1
""").show()
