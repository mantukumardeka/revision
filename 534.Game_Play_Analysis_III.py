

#-- Write an SQL query that reports for each player and date,
#-- the total number of games played by the player until that date (cumulative sum).
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

df=activity_df.withColumn("games_played_so_far",sum(col("games_played")).over( Window.partitionBy(col("player_id")).orderBy(col("event_date"))  ) )

df.show()


print("SQL")

# SELECT player_id, event_date,
#     SUM(games_played) OVER (PARTITION BY player_id ORDER BY event_date) AS games_played_so_far
# FROM Activity_534;