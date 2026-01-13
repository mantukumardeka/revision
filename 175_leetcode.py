# Revision SQL:
#
# Input:
# Person table:
# +----------+----------+-----------+
# | personId | lastName | firstName |
# +----------+----------+-----------+
# | 1        | Wang     | Allen     |
# | 2        | Alice    | Bob       |
# +----------+----------+-----------+
# Address table:
# +-----------+----------+---------------+------------+
# | addressId | personId | city          | state      |
# +-----------+----------+---------------+------------+
# | 1         | 2        | New York City | New York   |
# | 2         | 3        | Leetcode      | California |
# +-----------+----------+---------------+------------+
# Output:
# +-----------+----------+---------------+----------+
# | firstName | lastName | city          | state    |
# +-----------+----------+---------------+----------+
# | Allen     | Wang     | Null          | Null     |
# | Bob       | Alice    | New York City | New York |
# +-----------+----------+---------------+----------+
#


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CombineTables").getOrCreate()

# Person table
person_data = [
    (1, "Wang", "Allen"),
    (2, "Alice", "Bob")
]
person_df = spark.createDataFrame(person_data, ["personId", "lastName", "firstName"])

# Address table
address_data = [
    (1, 2, "New York City", "New York"),
    (2, 3, "Leetcode", "California")
]
address_df = spark.createDataFrame(address_data, ["addressId", "personId", "city", "state"])


person_df.show()

address_df.show()

finaldf=person_df.join(address_df,person_df.personId==address_df.personId,"left")

finaldf.show()


print("USING SQL")

person_df.createTempView("Person")

address_df.createTempView("Address")

spark.sql("select p.firstName,p.lastName,a.city,a.state from Person p left join Address a on a.personId=p.personId").show()