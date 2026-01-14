# Input:
# Employee table:
# +----+--------+
# | id | salary |
# +----+--------+
# | 1  | 100    |
# | 2  | 200    |
# | 3  | 300    |
# +----+--------+
# Output:
# +---------------------+
# | SecondHighestSalary |
# +---------------------+
# | 200                 |
# +---------------------+


from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("MaxSalary").getOrCreate()

data=[(1,100),(2,200),(3,300)]
schema=["id", "salary"]

employee_df=spark.createDataFrame(data,schema)

employee_df.show()

print("Using PySPARK-Get 2nd Highest Salary")

from pyspark.sql.functions import *
from pyspark.sql.window import Window

salary_df=employee_df.withColumn("rn", dense_rank().over(Window.orderBy(employee_df.salary.desc()))).filter(col("rn")==2)


salary_df.show()

print("Using SQL")

employee_df.createTempView("emp")

spark.sql("select max(salary) from emp where salary< (select max(salary) from emp) ").show()


