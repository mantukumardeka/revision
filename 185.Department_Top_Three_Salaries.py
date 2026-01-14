# Input:
# Employee table:
# +----+-------+--------+--------------+
# | id | name  | salary | departmentId |
# +----+-------+--------+--------------+
# | 1  | Joe   | 85000  | 1            |
# | 2  | Henry | 80000  | 2            |
# | 3  | Sam   | 60000  | 2            |
# | 4  | Max   | 90000  | 1            |
# | 5  | Janet | 69000  | 1            |
# | 6  | Randy | 85000  | 1            |
# | 7  | Will  | 70000  | 1            |
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
# | IT         | Max      | 90000  |
# | IT         | Joe      | 85000  |
# | IT         | Randy    | 85000  |
# | IT         | Will     | 70000  |
# | Sales      | Henry    | 80000  |
# | Sales      | Sam      | 60000  |
# +------------+----------+--------+
# Explanation:
# In the IT department:
# - Max earns the highest unique salary
# - Both Randy and Joe earn the second-highest unique salary
# - Will earns the third-highest unique salary
#
# In the Sales department:
# - Henry earns the highest salary
# - Sam earns the second-highest salary
# - There is no third-highest salary as there are only two employees



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EmployeeDepartment2").getOrCreate()

# Employee DataFrame
employee_data = [
    (1, "Joe", 85000, 1),
    (2, "Henry", 80000, 2),
    (3, "Sam", 60000, 2),
    (4, "Max", 90000, 1),
    (5, "Janet", 69000, 1),
    (6, "Randy", 85000, 1),
    (7, "Will", 70000, 1)
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
join_df=employee_df.alias("e").join(department_df.alias("d"), col("e.departmentId")==col("d.id"),'inner'    )\
    .select(col("e.name").alias("Employee"),col("d.name").alias("Department"),col("e.salary").alias("Salary") )

join_df.show()


rank_df=join_df.withColumn("rnk", dense_rank().over(

    Window.partitionBy("Department").orderBy(col("Salary").desc())

))

rank_df.show()

find_df=rank_df.filter(col("rnk")<=3).select("Employee","Department","Salary")

find_df.show()


#SQL
# #
# select Department,Employee,Salary from ( select e.name as 'Employee',e.salary, d.name as 'Department',dense_rank() over( partition by d.name order by  e.salary desc )
# as rnk  from Employee e join Department d on e.departmentId=d.id
# ) a where rnk<=3;