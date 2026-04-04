from common import (
    remove_extra_spaces,
    filter_senior_citizen,
    get_row_count,
    get_column_names,
    count_nulls
)

# -----------------------------------
# Test 1 : Transformation Test
# -----------------------------------
def test_remove_extra_spaces(spark_session):

    sample_data = [
        {"name": "John    D.", "age": 30},
        {"name": "Alice   G.", "age": 25},
        {"name": "Bob  T.", "age": 35}
    ]

    df = spark_session.createDataFrame(sample_data)

    transformed_df = remove_extra_spaces(df, "name")

    expected_data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35}
    ]

    expected_df = spark_session.createDataFrame(expected_data)

    assert transformed_df.collect() == expected_df.collect()


# -----------------------------------
# Test 2 : Row Count Test
# -----------------------------------
def test_row_count(spark_session):

    sample_data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 28}
    ]

    df = spark_session.createDataFrame(sample_data)

    expected_count = 4

    assert get_row_count(df) == expected_count


# -----------------------------------
# Test 3 : Schema Validation Test
# -----------------------------------
def test_schema_validation(spark_session):

    sample_data = [
        {"name": "John D.", "age": 30}
    ]

    df = spark_session.createDataFrame(sample_data)

    expected_columns = ["name", "age"]

    assert set(get_column_names(df)) == set(expected_columns)


# -----------------------------------
# Test 4 : Data Quality Test
# -----------------------------------
def test_null_check(spark_session):

    sample_data = [
        {"name": "John D.", "age": 30},
        {"name": None, "age": 25},
        {"name": "Bob T.", "age": 35}
    ]

    df = spark_session.createDataFrame(sample_data)

    expected_null_count = 1

    assert count_nulls(df, "name") == expected_null_count


# -----------------------------------
# Test 5 : Business Rule Test
# -----------------------------------
def test_senior_citizen_filter(spark_session):

    sample_data = [
        {"name": "John D.", "age": 60},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 65},
        {"name": "Eve A.", "age": 28}
    ]

    df = spark_session.createDataFrame(sample_data)

    filtered_df = filter_senior_citizen(df, "age")

    expected_count = 2

    assert filtered_df.count() == expected_count