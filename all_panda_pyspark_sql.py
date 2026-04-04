from numpy.ma.core import inner
#========Using Panda========
# import pandas as pd
#
# df = pd.DataFrame({
#     "ID": [10, 11, 12],
#     "Salary": [2000, 5000, 3000]
# })
#
# print(df)
#
#
# df1=df["Salary"].drop_duplicates().nlargest(2).iloc[-1]
# #
# # print(f"2nd highest Salary :{df1}")
#
#
# #=========Using PySpark=========
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import dense_rank
#
# # Create Spark session
# spark = SparkSession.builder.appName("Example").getOrCreate()
#
# # Data
# data = [
#     (10, 2000),
#     (11, 5000),
#     (12, 3000)
# ]
#
# # Create DataFrame
# df = spark.createDataFrame(data, ["ID", "Salary"])
#
# # Show Data
# df.show()
#
# from pyspark.sql.window import Window
# from pyspark.sql.functions import col


#wind_spac=Window.orderBy(col("salary").desc())

# df_2nd=df.withColumn("rank", dense_rank().over(Window.orderBy(col("Salary").desc()) )   ).filter(col("rank")==2).select(col("Salary"))
# df_2nd.show()

# #===============MAX==JOIN========
#
import pandas as pd

# Employee DataFrame
emp = pd.DataFrame({
    "ID": [10,20,30,40,50,60,70,80,90,100],
    "Salary": [1000,5000,3000,7000,2000,8000,6000,9000,4000,5500],
    "DeptID": [2,3,2,1,1,3,2,1,3,2]
})

# Department DataFrame
dept = pd.DataFrame({
    "ID": [1,2,3],
    "DeptName": ["Marketing","IT","Finance"]
})

# Merge + GroupBy
result = emp.merge(dept, left_on="DeptID", right_on="ID") \
            .groupby("DeptName")["Salary"].max().reset_index(name="Max_Salary")

print(result)


#========USING PySPARK=========

# from pyspark.sql import SparkSession
#
# from pyspark.sql.functions import *
# from pyspark.sql.window import Window
#
# # Start Spark
# spark = SparkSession.builder.appName("EmployeeDept").getOrCreate()
#
# # Employee Data
# emp_data = [
# (10,1000,2),
# (20,5000,3),
# (30,3000,2),
# (40,7000,1),
# (50,2000,1),
# (60,8000,3),
# (70,6000,2),
# (80,9000,1),
# (90,4000,3),
# (100,5500,2)
# ]
#
# emp = spark.createDataFrame(emp_data, ["ID","Salary","DeptID"])
#
# # Department Data
# dept_data = [
# (1,"Marketing"),
# (2,"IT"),
# (3,"Finance")
# ]
#
# dept = spark.createDataFrame(dept_data, ["ID","DeptName"])
#
# # Show Data
# emp.show()
# dept.show()
#
#
# join_df=emp.alias("e").join(dept.alias("d") ,col("e.DeptId")==col("d.Id"),"inner")
#
# ### USING GROUP BY
# #join_df.show()
#
# max_df = join_df.groupBy("DeptName") \
#                 .agg(max("Salary").alias("Max_Salary")) \
#                 .orderBy(col("Max_Salary").desc())
#
# max_df.show()
# ### USING Partition By:
#
# rank_df = join_df.withColumn(
#     "rank",
#     dense_rank().over(
#         Window.partitionBy("DeptName").orderBy(col("Salary").desc())
#     )
# ).filter(col("rank") == 1) \
#  .select("DeptName", "Salary")
#
# rank_df.show()


===============