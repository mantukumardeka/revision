# Input:
# Weather table:
# +----+------------+-------------+
# | id | recordDate | temperature |
# +----+------------+-------------+
# | 1  | 2015-01-01 | 10          |
# | 2  | 2015-01-02 | 25          |
# | 3  | 2015-01-03 | 20          |
# | 4  | 2015-01-04 | 30          |
# +----+------------+-------------+
# Output:
# +----+
# | id |
# +----+
# | 2  |
# | 4  |
# +----+
# Explanation:
# In 2015-01-02, the temperature was higher than the previous day (10 -> 25).
# In 2015-01-04, the temperature was higher than the previous day (20 -> 30).

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder.appName("Weather").getOrCreate()

data = [
    (1, "2015-01-01", 10),
    (2, "2015-01-02", 25),
    (3, "2015-01-03", 20),
    (4, "2015-01-04", 30)
]

columns = ["id", "recordDate", "temperature"]

weather_df = spark.createDataFrame(data, columns) \
    .withColumn("recordDate", to_date("recordDate"))

weather_df.show()

from pyspark.sql.functions import *
from pyspark.sql.window import Window

wind_df=weather_df.withColumn("prev", lag("temperature").over(Window.orderBy(col("recordDate"))))\
    .filter(col("temperature")>col("prev")).select("id")

wind_df.show()

print("using SQL")

# SELECT w1.Id
# FROM Weather w1, Weather w2
# WHERE DATEDIFF(w1.RecordDate, w2.RecordDate) = 1
#   AND w1.Temperature > w2.Temperature;