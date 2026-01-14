# Input:
# Employee table:
# +----+-------+--------+-----------+
# | id | name  | salary | managerId |
# +----+-------+--------+-----------+
# | 1  | Joe   | 70000  | 3         |
# | 2  | Henry | 80000  | 4         |
# | 3  | Sam   | 60000  | Null      |
# | 4  | Max   | 90000  | Null      |
# +----+-------+--------+-----------+
# Output:
# +----------+
# | Employee |
# +----------+
# | Joe      |
# +----------+
# Explanation: Joe is the only employee who earns more than his manager.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EmployeeDF").getOrCreate()

data = [
    (1, "Joe", 70000, 3),
    (2, "Henry", 80000, 4),
    (3, "Sam", 60000, None),
    (4, "Max", 90000, None)
]

schema = ["id", "name", "salary", "managerId"]

emp_df = spark.createDataFrame(data, schema)

emp_df.show()


from pyspark.sql.functions import *


join_df=emp_df.alias("e").join(emp_df.alias("m"),col("m.id")==col("e.managerId") ,"inner")\
    .filter(col("e.salary")>col("m.salary")) \
    .select(col("e.name").alias("Employee"))

join_df.show()