# Input:
# Employee table:
# +----+-------+--------+--------------+
# | id | name  | salary | departmentId |
# +----+-------+--------+--------------+
# | 1  | Joe   | 70000  | 1            |
# | 2  | Jim   | 90000  | 1            |
# | 3  | Henry | 80000  | 2            |
# | 4  | Sam   | 60000  | 2            |
# | 5  | Max   | 90000  | 1            |
# +----+-------+--------+--------------+
# Department table:
# +----+-------+
# | id | name  |
# +----+-------+
# | 1  | IT    |
# | 2  | Sales |
# +----+-------+
# Output:
# +------------+----------+--------+
# | Department | Employee | Salary |
# +------------+----------+--------+
# | IT         | Jim      | 90000  |
# | Sales      | Henry    | 80000  |
# | IT         | Max      | 90000  |
# +------------+----------+--------+
# Explanation: Max and Jim both have the highest salary in the IT department and Henry has the highest salary in the Sales department.


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EmployeeDepartment").getOrCreate()

# Employee DataFrame
employee_data = [
    (1, "Joe", 70000, 1),
    (2, "Jim", 90000, 1),
    (3, "Henry", 80000, 2),
    (4, "Sam", 60000, 2),
    (5, "Max", 90000, 1)
]

employee_cols = ["id", "name", "salary", "departmentId"]
employee_df = spark.createDataFrame(employee_data, employee_cols)

# Department DataFrame
department_data = [
    (1, "IT"),
    (2, "Sales")
]

department_cols = ["id", "name"]
department_df = spark.createDataFrame(department_data, department_cols)

# Show DataFrames
employee_df.show()
department_df.show()

from pyspark.sql.functions import *
from pyspark.sql.window import Window

join_df=employee_df.alias("e").join(department_df.alias("d"),col("e.departmentId")==col("d.id") , 'inner') \
    .select(col("e.name").alias("Employee"),col("d.name").alias("Department"),col("e.salary").alias("Salary"),)
print("JOIND DF")
join_df.show()
#
rank_df=join_df.withColumn("rnk",rank().over(Window.partitionBy("Department").orderBy(col("Salary").desc())))

rank_df.show()

final_df=rank_df.select("Employee","Department","Salary").filter(col("rnk")==1)

final_df.show()



# USING SQL
#select Department,Employee,salary from (select d.name as "Department",e.name as "Employee",e.salary ,rank() over(partition by d.name order by e.salary desc) as rnk  from Employee e join Department d  on e.departmentId=d.id ) a where rnk=1;
