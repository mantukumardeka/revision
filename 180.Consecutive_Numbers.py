# Example 1:
#
# Input:
# Logs table:
# +----+-----+
# | id | num |
# +----+-----+
# | 1  | 1   |
# | 2  | 1   |
# | 3  | 1   |
# | 4  | 2   |
# | 5  | 1   |
# | 6  | 2   |
# | 7  | 2   |
# +----+-----+
# Output:
# +-----------------+
# | ConsecutiveNums |
# +-----------------+
# | 1               |
# +-----------------+


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NumDF").getOrCreate()

data = [
    (1, 1),
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 1),
    (6, 2),
    (7, 2)
]

schema = ["id", "num"]

num_df = spark.createDataFrame(data, schema)

num_df.show()


from pyspark.sql.functions import *
from pyspark.sql.window import Window

final_df=num_df.withColumn("PrevNum",lag("num").over(Window.orderBy(col("id"))) ).withColumn("NextNum",lead("num").over(Window.orderBy(col("id"))))

final_df.show()

con_df=final_df.filter(
    (col("num")==col("NextNum")) &
    (col("num")==col("PrevNum"))

).select("num").distinct()

con_df.show()



print("Using SQL")

num_df.createTempView("numbers")

spark.sql("""

with tt as
(select id, num, lag(num) over (order by id ) as prevnum, lead(num) over(order by id) as nextnum from numbers)
select num as ConsecutiveNums from tt where num=prevnum and num=nextnum


""").show()



