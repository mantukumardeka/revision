from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName("JoinsDemo").getOrCreate()

# ── Create table1 ──────────────────────────────────────────
schema1 = StructType([StructField("Col1", IntegerType(), nullable=True)])

data1 = [(3,), (1,), (1,), (1,), (None,), (0,)]
table1 = spark.createDataFrame(data1, schema=schema1)
table1.createOrReplaceTempView("table1")

# ── Create table2 ──────────────────────────────────────────
schema2 = StructType([StructField("Col1", IntegerType(), nullable=True)])

data2 = [(1,), (0,), (None,), (None,), (0,), (1,), (1,), (None,), (2,)]
table2 = spark.createDataFrame(data2, schema=schema2)
table2.createOrReplaceTempView("table2")

# ── Rename columns to avoid ambiguity after join ───────────
t1 = table1.withColumnRenamed("Col1", "t1_Col1")
t2 = table2.withColumnRenamed("Col1", "t2_Col1")

# ── Inner Join ─────────────────────────────────────────────
# Only matching rows (NULLs never match in standard join)
inner = t1.join(t2, t1.t1_Col1 == t2.t2_Col1, "inner")
print("=== INNER JOIN ===")
inner.show()

# ── Left Join ──────────────────────────────────────────────
# All rows from table1; NULLs in t2 side where no match
left = t1.join(t2, t1.t1_Col1 == t2.t2_Col1, "left")
print("=== LEFT JOIN ===")
left.show()

# ── Full Outer Join ────────────────────────────────────────
# All rows from both tables; NULLs where no match on either side
full = t1.join(t2, t1.t1_Col1 == t2.t2_Col1, "full")
print("=== FULL OUTER JOIN ===")
full.show()