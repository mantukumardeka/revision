# Input:
# Customers table:
# +----+-------+
# | id | name  |
# +----+-------+
# | 1  | Joe   |
# | 2  | Henry |
# | 3  | Sam   |
# | 4  | Max   |
# +----+-------+
# Orders table:
# +----+------------+
# | id | customerId |
# +----+------------+
# | 1  | 3          |
# | 2  | 1          |
# +----+------------+
# Output:
# +-----------+
# | Customers |
# +-----------+
# | Henry     |
# | Max       |
# +-----------+

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomersOrders").getOrCreate()

# Customers data
customers_data = [
    (1, "Joe"),
    (2, "Henry"),
    (3, "Sam"),
    (4, "Max")
]

customers_cols = ["id", "name"]
customers_df = spark.createDataFrame(customers_data, customers_cols)

# Orders data
orders_data = [
    (1, 3),
    (2, 1)
]

orders_cols = ["id", "customerId"]
orders_df = spark.createDataFrame(orders_data, orders_cols)

# Show DataFrames
customers_df.show()
orders_df.show()

from pyspark.sql.functions import *

final_df=customers_df.join(orders_df,customers_df.id==orders_df.customerId,"left")\
    .filter(orders_df.customerId.isNull()).select(col("name").alias("Customers"))

final_df.show()


print("OR USING LEFT ANTI")

final1_df=customers_df.join(orders_df,customers_df.id==orders_df.customerId,"left_anti")

final1_df.show()

#
# select c.name as 'Customers' from Customers c left join Orders o on c.id=o.customerId where o.customerId is Null;