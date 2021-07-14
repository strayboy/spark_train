from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, StructType, StructField


def basic(spark: SparkSession):
    df = spark.read.json(
        "file:///Users/gaoboce/software/spark-2.3.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()


def schema_Inferring_example(spark: SparkSession):
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile(
        "file:///Users/gaoboce/software/spark-2.3.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teenNames:
        print(name)
    # Name: Justin


def programmatic_schema_example(spark: SparkSession):
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("file:///Users/gaoboce/software/spark-2.3.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    # Each line is converted to a tuple.
    people = parts.map(lambda p: (p[0], p[1].strip()))

    # The schema is encoded in a string.
    schemaString = "name age"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaPeople = spark.createDataFrame(people, schema)

    # Creates a temporary view using the DataFrame
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT name FROM people")

    results.show()


if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark0801").getOrCreate()
    basic(spark)
    spark.stop()
