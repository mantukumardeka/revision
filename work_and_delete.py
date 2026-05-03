from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataCleaningPractice").getOrCreate()

data = [
    (1, " Alice ", "goa", None, 25, "5000", "2025-01-01", ["Python", "SQL"]),
    (2, "", " mumbai ", 2000, -5, "abc", "2025/02/01", ["Java", "Python"]),
    (3, None, None, None, 30, "70000", "2025-03-01", None),
    (4, "Bob@", "delhi", 150000, 40, "90000", "2025-04-01", ["Scala"]),
    (4, "Bob@", "delhi", 150000, 40, "90000", "2025-04-01", ["Scala"]),  # duplicate
]

columns = ["id", "name", "city", "salary", "age", "salary_str", "date", "skills"]

df = spark.createDataFrame(data, columns)
df.show(truncate=False)