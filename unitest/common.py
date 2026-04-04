from pyspark.sql.functions import col, regexp_replace

# Remove additional spaces in name
def remove_extra_spaces(df, column_name):
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))
    return df_transformed

# Filter Senior Citizen
def filter_senior_citizen(df, column_name):
    df_filtered = df.filter(col(column_name) >= 60)
    return df_filtered

# row-count
def get_row_count(df):
    return df.count()


#  testing others-

from pyspark.sql.functions import col, regexp_replace

# -----------------------------
# 1. Transformation Function
# -----------------------------
# Remove extra spaces in name
def remove_extra_spaces(df, column_name):
    return df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))


# -----------------------------
# 2. Business Rule
# -----------------------------
# Filter Senior Citizens
def filter_senior_citizen(df, column_name):
    return df.filter(col(column_name) >= 60)


# -----------------------------
# 3. Row Count Function
# -----------------------------
def get_row_count(df):
    return df.count()


# -----------------------------
# 4. Schema Validation
# -----------------------------
def get_column_names(df):
    return df.columns


# -----------------------------
# 5. Data Quality Check
# -----------------------------
# Count null values in column
def count_nulls(df, column_name):
    return df.filter(col(column_name).isNull()).count()
