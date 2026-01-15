# Input:
# Trips table:
# +----+-----------+-----------+---------+---------------------+------------+
# | id | client_id | driver_id | city_id | status              | request_at |
# +----+-----------+-----------+---------+---------------------+------------+
# | 1  | 1         | 10        | 1       | completed           | 2013-10-01 |
# | 2  | 2         | 11        | 1       | cancelled_by_driver | 2013-10-01 |
# | 3  | 3         | 12        | 6       | completed           | 2013-10-01 |
# | 4  | 4         | 13        | 6       | cancelled_by_client | 2013-10-01 |
# | 5  | 1         | 10        | 1       | completed           | 2013-10-02 |
# | 6  | 2         | 11        | 6       | completed           | 2013-10-02 |
# | 7  | 3         | 12        | 6       | completed           | 2013-10-02 |
# | 8  | 2         | 12        | 12      | completed           | 2013-10-03 |
# | 9  | 3         | 10        | 12      | completed           | 2013-10-03 |
# | 10 | 4         | 13        | 12      | cancelled_by_driver | 2013-10-03 |
# +----+-----------+-----------+---------+---------------------+------------+
# Users table:
# +----------+--------+--------+
# | users_id | banned | role   |
# +----------+--------+--------+
# | 1        | No     | client |
# | 2        | Yes    | client |
# | 3        | No     | client |
# | 4        | No     | client |
# | 10       | No     | driver |
# | 11       | No     | driver |
# | 12       | No     | driver |
# | 13       | No     | driver |
# +----------+--------+--------+
# Output:
# +------------+-------------------+
# | Day        | Cancellation Rate |
# +------------+-------------------+
# | 2013-10-01 | 0.33              |
# | 2013-10-02 | 0.00              |
# | 2013-10-03 | 0.50              |
# +------------+-------------------+
# Explanation:
# On 2013-10-01:
#   - There were 4 requests in total, 2 of which were canceled.
#   - However, the request with Id=2 was made by a banned client (User_Id=2), so it is ignored in the calculation.
#   - Hence there are 3 unbanned requests in total, 1 of which was canceled.
#   - The Cancellation Rate is (1 / 3) = 0.33
# On 2013-10-02:
#   - There were 3 requests in total, 0 of which were canceled.
#   - The request with Id=6 was made by a banned client, so it is ignored.
#   - Hence there are 2 unbanned requests in total, 0 of which were canceled.
#   - The Cancellation Rate is (0 / 2) = 0.00
# On 2013-10-03:
#   - There were 3 requests in total, 1 of which was canceled.
#   - The request with Id=8 was made by a banned client, so it is ignored.
#   - Hence there are 2 unbanned request in total, 1 of which were canceled.
#   - The Cancellation Rate is (1 / 2) = 0.50


from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder.appName("TripsUsers").getOrCreate()

# Trips DataFrame
trips_data = [
    (1, 1, 10, 1, "completed", "2013-10-01"),
    (2, 2, 11, 1, "cancelled_by_driver", "2013-10-01"),
    (3, 3, 12, 6, "completed", "2013-10-01"),
    (4, 4, 13, 6, "cancelled_by_client", "2013-10-01"),
    (5, 1, 10, 1, "completed", "2013-10-02"),
    (6, 2, 11, 6, "completed", "2013-10-02"),
    (7, 3, 12, 6, "completed", "2013-10-02"),
    (8, 2, 12, 12, "completed", "2013-10-03"),
    (9, 3, 10, 12, "completed", "2013-10-03"),
    (10, 4, 13, 12, "cancelled_by_driver", "2013-10-03")
]

trips_cols = ["id", "client_id", "driver_id", "city_id", "status", "request_at"]

trips_df = spark.createDataFrame(trips_data, trips_cols) \
    .withColumn("request_at", to_date("request_at"))

# Users DataFrame
users_data = [
    (1, "No", "client"),
    (2, "Yes", "client"),
    (3, "No", "client"),
    (4, "No", "client"),
    (10, "No", "driver"),
    (11, "No", "driver"),
    (12, "No", "driver"),
    (13, "No", "driver")
]

users_cols = ["users_id", "banned", "role"]
users_df = spark.createDataFrame(users_data, users_cols)

# Show DataFrames
trips_df.show()
users_df.show()

from pyspark.sql.functions import col, when, sum as _sum, count, round

result_df = (
    trips_df.alias("t")
    # Join client (unbanned)
    .join(
        users_df.alias("c"),
        (col("t.client_id") == col("c.users_id")) &
        (col("c.banned") == "No"),
        "inner"
    )
    # Join driver (unbanned)
    .join(
        users_df.alias("d"),
        (col("t.driver_id") == col("d.users_id")) &
        (col("d.banned") == "No"),
        "inner"
    )
    # Date filter
    .filter(
        col("t.request_at").between("2013-10-01", "2013-10-03")
    )
    # Aggregation
    .groupBy(col("t.request_at").alias("Day"))
    .agg(
        round(
            _sum(
                when(col("t.status").like("cancelled%"), 1).otherwise(0)
            ) / count("*"),
            2
        ).alias("Cancellation Rate")
    )
    .orderBy("Day")
)

result_df.show()



print("USING SQL")



# SELECT
#     t.request_at AS Day,
#     ROUND(
#         SUM(CASE WHEN t.status LIKE 'cancelled%' THEN 1 ELSE 0 END) / COUNT(*),
#         2
#     ) AS "Cancellation Rate"
# FROM Trips t
# JOIN Users c
#     ON t.client_id = c.users_id AND c.banned = 'No'
# JOIN Users d
#     ON t.driver_id = d.users_id AND d.banned = 'No'
# WHERE t.request_at BETWEEN '2013-10-01' AND '2013-10-03'
# GROUP BY t.request_at
# ORDER BY t.request_at;