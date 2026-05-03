from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

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
df_orders.show()
#
# print("Order Items DataFrame:")
# df_order_items.show()

#2n Highest salary
wind_spec=Window.orderBy(col("salary").desc())
df1=df_employee.withColumn("rnk", row_number().over(wind_spec)).select("Salary").filter(col("rnk")==2)
#df1.show()
#2.• Write a SQL query to find employees who have the same manager.

df2=df_employee.groupby("manager_id").agg(count("*").alias("cnt")).filter(col("cnt")>1)
#df2.show()

#3• Write a query to find the first and last record for each employee based on the 'hire_date column.

# df3=df_employee.withColumn("rnkf", row_number().over(Window.orderBy(col("hire_date").desc())  ))\
#     .withColumn("rnkl", row_number().over(Window.orderBy(col("hire_date"))))\
#     .filter((col("rnkf") == 1) | (col("rnkl") == 1))
#
# df3.show()


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# 1. Define Window specs
# Partitioning by emp_id ensures we look at each employee's history independently
win_first = Window.partitionBy("emp_id").orderBy(col("hire_date").asc())
win_last = Window.partitionBy("emp_id").orderBy(col("hire_date").desc())

# 2. Add ranking columns and filter
# We use '|' (OR) because we want the row if it's the first OR the last record
df_first_last = df_employee.withColumn("rnk_first", row_number().over(win_first)) \
                           .withColumn("rnk_last", row_number().over(win_last)) \
                           .filter((col("rnk_first") == 1) | (col("rnk_last") == 1))

#df_first_last.show()

#4• Write a query to find the most recent transaction for each customer.

df4=df_orders.withColumn("rn", row_number().over(Window.orderBy(col("order_date").desc()))).filter(col("rn")==1)

#df4.show()

#5 • Write a query to find the total salary of each department and display

df5=df_employee.groupby("dept_id").agg(sum(col('salary')).alias("Total_sal")).filter(col("Total_sal")>200000)
#df5.show()

# 6• Write a query to find the running total of orders for each customer sorted by order date.

df6=df_orders.withColumn("total_order", sum("order_amount")\
 .over(Window.partitionBy("customer_id","order_date").orderBy(col("order_date").desc())))

#df6.show()

#7• Write a query to get the total number of employees hired per month and year.

df7=df_employee.groupby(month("hire_date").alias("Months"),year("hire_date").alias("year")).count()

#df7.show()

#8. Write a query to display all employees who earn more than the average salary for their department.

df9=df_employee.groupby("dept_id").agg(avg(col("salary")).alias("avg_sal"))

df_employee.join(df9,"dept_id").filter(col("salary")>col("avg_sal"))

#9• Write a query to list all the employees who are also managers.

manager=df_employee.select(col("manager_id")).distinct()
df_employee.join(manager,df_employee.emp_id==manager.manager_id).select("emp_id","name","salary")

#10• Write a query to find employees (NAMES) who have joined in the same month and year. (AA)

# df10=df_employee.withColumn("cnt", row_number().over(Window.partitionBy(year("hire_date"),month("hire_date")).orderBy("emp_id")))
# df11=df10.filter(col("cnt")>1)
# df11.show()

df12=df_employee.groupby(year("hire_date"),month("hire_date")).count().filter(col("count")>1)
#df12.show()

#11.Write a SQL query to get a list of employees who are older than the average age of employees in their department.

df13=df_employee.groupby("dept_id").agg(avg(col("age")).alias("avgage"))
df_employee.join(df13,"dept_id").filter(col("age")>col("avgage"))

#12• Write a query to display the employee(s) with the longest tenure at the company.

df_employee.orderBy(col("hire_date").asc()).limit(1)

#13• Write a query to delete all records from a table where the column value is NULL.

df_employee.filter(col("manager_id").isNotNull()).show()

