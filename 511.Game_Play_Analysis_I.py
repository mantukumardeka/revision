# Input:
# Activity table:
# +-----------+-----------+------------+--------------+
# | player_id | device_id | event_date | games_played |
# +-----------+-----------+------------+--------------+
# | 1         | 2         | 2016-03-01 | 5            |
# | 1         | 2         | 2016-05-02 | 6            |
# | 2         | 3         | 2017-06-25 | 1            |
# | 3         | 1         | 2016-03-02 | 0            |
# | 3         | 4         | 2018-07-03 | 5            |
# +-----------+-----------+------------+--------------+
# Output:
# +-----------+-------------+
# | player_id | first_login |
# +-----------+-------------+
# | 1         | 2016-03-01  |
# | 2         | 2017-06-25  |
# | 3         | 2016-03-02  |
# +-----------+-------------+
#
# Write a solution to find the first login date for each player.



from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-05-02", 6),
    (2, 3, "2017-06-25", 1),
    (3, 1, "2016-03-02", 0),
    (3, 4, "2018-07-03", 5)
]

columns = ["player_id", "device_id", "event_date", "games_played"]

activity_df = spark.createDataFrame(data, columns) \
    .withColumn("event_date", col("event_date").cast("date"))

activity_df.show()

from pyspark.sql.window import Window

final_df=activity_df.groupby("player_id")\
    .agg(min(col("event_date")).alias("firstlogin"))

final_df.show()

print("USING SQL==")

#select player_id, min(event_date) as first_login from Activity_511 group by player_id;