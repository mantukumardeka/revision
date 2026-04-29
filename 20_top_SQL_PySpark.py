from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("SQLtoPySparkPractice").getOrCreate()

# Create employee20 Data
# Ensuring every tuple has exactly 7 elements
emp_data = [
    (1, 'Alice', 60000, 10, '2023-01-15', 3, 30),
    (2, 'Bob', 75000, 20, '2023-02-10', 3, 35),
    (3, 'Charlie', 80000, 10, '2022-05-20', None, 45), # Fixed row
    (4, 'David', 55000, 20, '2024-03-05', 3, 28),
    (5, 'Eve', 90000, 10, '2023-01-15', None, 50),
    (6, 'Frank', 60000, 10, '2023-06-01', 5, 32),
    (7, 'Grace', 70000, 30, '2022-11-20', 5, 40),
    (8, 'Heidi', 85000, 20, '2021-08-15', 2, 38),
    (9, 'Ivan', None, 30, '2024-01-10', 7, 29),
    (10, 'Judy', 95000, 30, '2020-12-12', 7, 42)
]

emp_schema = ["emp_id", "name", "salary", "dept_id", "hire_date", "manager_id", "age"]
df_employee = spark.createDataFrame(emp_data, emp_schema)

# Create orders20 Data
orders_data = [
    (101, 1, '2025-01-05', 200),
    (102, 2, '2025-01-15', 150),
    (103, 1, '2025-02-10', 300),
    (104, 3, '2025-02-20', 450),
    (105, 2, '2025-03-05', 200),
    (106, 1, '2025-03-12', 100),
    (107, 4, '2025-04-01', 500),
    (108, 3, '2025-04-15', 250),
    (109, 2, '2025-05-10', 350),
    (110, 4, '2025-06-20', 150)
]

orders_schema = ["order_id", "customer_id", "order_date", "order_amount"]
df_orders = spark.createDataFrame(orders_data, orders_schema)

# Create order_items20 Data
oi_data = [(101, 1), (101, 2), (102, 2), (102, 3), (103, 1), (103, 3), (104, 1), (104, 2), (105, 4), (106, 1)]
oi_schema = ["order_id", "product_id"]
df_order_items = spark.createDataFrame(oi_data, oi_schema)

# Verify the data
print("Employee DataFrame:")
df_employee.show()

# print("Orders DataFrame:")
# df_orders.show()
#
# print("Order Items DataFrame:")
# df_order_items.show()

