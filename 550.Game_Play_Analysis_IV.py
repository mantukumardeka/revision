
# Write a solution to report the fraction of players that logged in again on the day after the day they first logged in, rounded to 2 decimal places. In other words, you need to determine the number of players who logged in on the day immediately following their initial login, and divide it by the number of total players.
#
# Input:
# Activity table:
# +-----------+-----------+------------+--------------+
# | player_id | device_id | event_date | games_played |
# +-----------+-----------+------------+--------------+
# | 1         | 2         | 2016-03-01 | 5            |
# | 1         | 2         | 2016-03-02 | 6            |
# | 2         | 3         | 2017-06-25 | 1            |
# | 3         | 1         | 2016-03-02 | 0            |
# | 3         | 4         | 2018-07-03 | 5            |
# +-----------+-----------+------------+--------------+
# Output:
# +-----------+
# | fraction  |
# +-----------+
# | 0.33      |
# +-----------+

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

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("Game_Play_Analysis_IV").getOrCreate()

# Input data
data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-03-02", 6),
    (2, 3, "2017-06-25", 1),
    (3, 1, "2016-03-02", 0),
    (3, 4, "2018-07-03", 5)
]

columns = ["player_id", "device_id", "event_date", "games_played"]

# Create DataFrame and convert event_date to DateType
activity_df = spark.createDataFrame(data, columns) \
    .withColumn("event_date", to_date("event_date"))

print("ACTIVITY TABLE")
activity_df.show()

# Step 1: Find first login date per player
df = activity_df.withColumn(
    "first_date",
    min("event_date").over(Window.partitionBy("player_id"))
)

print("FIRST LOGIN DATE PER PLAYER")
df.show()

# Step 2: Players who logged in the next day
next_day_df = df.filter(
    col("event_date") == date_add(col("first_date"), 1)
).select("player_id").distinct()

print("PLAYERS LOGGED IN NEXT DAY")
next_day_df.show()

# Step 3: Count players
total_players = activity_df.select("player_id").distinct().count()
next_day_players = next_day_df.count()

# Step 4: Calculate fraction using Python round
fraction = round(next_day_players / total_players, 2)

print("FINAL RESULT")
print("fraction =", fraction)


print("SQL")


# SELECT
#     ROUND(
#         COUNT(DISTINCT a.player_id) /
#         (SELECT COUNT(DISTINCT player_id) FROM Activity),
#         2
#     ) AS fraction
# FROM Activity a
# JOIN (
#     SELECT player_id, MIN(event_date) AS first_date
#     FROM Activity
#     GROUP BY player_id
# ) f
# ON a.player_id = f.player_id
# AND a.event_date = DATE_ADD(f.first_date, INTERVAL 1 DAY);