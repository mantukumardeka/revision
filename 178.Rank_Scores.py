# Example 1:
#
# Input:
# Scores table:
# +----+-------+
# | id | score |
# +----+-------+
# | 1  | 3.50  |
# | 2  | 3.65  |
# | 3  | 4.00  |
# | 4  | 3.85  |
# | 5  | 4.00  |
# | 6  | 3.65  |
# +----+-------+
# Output:
# +-------+------+
# | score | rank |
# +-------+------+
# | 4.00  | 1    |
# | 4.00  | 1    |
# | 3.85  | 2    |
# | 3.65  | 3    |
# | 3.65  | 3    |
# | 3.50  | 4    |
# +-------+------+


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ScoreDF").getOrCreate()

data = [
    (1, 3.50),
    (2, 3.65),
    (3, 4.00),
    (4, 3.85),
    (5, 4.00),
    (6, 3.65)
]

schema = ["id", "score"]

score_df = spark.createDataFrame(data, schema)

score_df.show()

from pyspark.sql.functions import *
from pyspark.sql.window import Window

final_df=score_df.withColumn("rank", dense_rank().over(Window.orderBy(col("score").desc())))

final_df.show()