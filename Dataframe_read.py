from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read a CSV file
df = spark.read.csv('path/to/file.csv', header=True, inferSchema=True)


# Read a JSON file
df = spark.read.json('path/to/file.json')


# Read a Parquet file
df = spark.read.parquet('path/to/file.parquet')


# Read a Hive table
df = spark.sql('SELECT * FROM my_table')


# Configure JDBC connection properties
jdbc_url = "jdbc:mysql://localhost:3306/my_database"
connection_properties = {
    "user": "my_username",
    "password": "my_password",
    "driver": "com.mysql.jdbc.Driver"
}

# Read a table from a JDBC connection
df = spark.read.jdbc(url=jdbc_url, table="my_table", properties=connection_properties)