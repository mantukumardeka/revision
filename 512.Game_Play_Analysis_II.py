
# -- Write a SQL query that reports the device that is first logged in for each player.
# +-----------+-----------+------------+--------------+
# | player_id | device_id | event_date | games_played |
# +-----------+-----------+------------+--------------+
# |         1 |         2 | 2016-03-01 |            5 |
# |         1 |         2 | 2016-05-02 |            6 |
# |         2 |         3 | 2017-06-25 |            1 |
# |         3 |         1 | 2016-03-02 |            0 |
# |         3 |         4 | 2018-07-03 |            5 |
# +-----------+-----------+------------+--------------+


from pyspark.sql import SparkSession
from pyspark.sql.functions import *



from pyspark.sql.window import Window

spark=SparkSession.builder.appName("kk").getOrCreate()

data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-05-02", 6),
    (2, 3, "2017-06-25", 1),
    (3, 1, "2016-03-02", 0),
    (3, 4, "2018-07-03", 5)
]

columns = ["player_id", "device_id", "event_date", "games_played"]

activity_df = spark.createDataFrame(data, columns)

activity_df.show()

from pyspark.sql.functions import *
from pyspark.sql.window import Window

w = Window.partitionBy("player_id").orderBy("event_date")

result_df = activity_df \
    .withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .select("player_id", "device_id")

result_df.show()


print("SQL")

# SELECT player_id, device_id
# FROM Activity_512
# WHERE (player_id, event_date) IN (
#     SELECT player_id, MIN(event_date)
#     FROM Activity_512
#     GROUP BY player_id);