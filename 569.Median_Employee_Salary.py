

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Employee_569").getOrCreate()

data = [
    (1, "A", 2341),
    (2, "A", 341),
    (3, "A", 15),
    (4, "A", 15314),
    (5, "A", 451),
    (6, "A", 513),
    (7, "B", 15),
    (8, "B", 13),
    (9, "B", 1154),
    (10, "B", 1345),
    (11, "B", 1221),
    (12, "B", 234),
    (13, "C", 2345),
    (14, "C", 2645),
    (15, "C", 2645),
    (16, "C", 2652),
    (17, "C", 65)
]

columns = ["Id", "Company", "Salary"]

employee_569_df = spark.createDataFrame(data, columns)

employee_569_df.show()
