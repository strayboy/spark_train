from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def get_grade(value):
    if 0 <= value <= 50:
        return "健康"
    elif value <= 100:
        return "中等"
    elif value <= 150:
        return "对敏感人群不健康"
    elif value <= 200:
        return "不健康"
    elif value <= 300:
        return "非常不健康"
    elif value <= 500:
        return "危险"
    elif value > 500:
        return "爆表"
    else:
        return None


grade_function_udf = udf(get_grade, StringType())

if __name__ == '__main__':
    spark = SparkSession.builder.appName("project").getOrCreate()

    data_2017 = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
        "/data/Beijing_2017_HourlyPM25_created20170803.csv").select("Year", "Month", "Day", "Hour",
                                                                    "Value", "QC Name")
    data_2016 = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
        "/data/Beijing_2016_HourlyPM25_created20170201.csv").select("Year", "Month", "Day", "Hour",
                                                                    "Value", "QC Name")
    data_2015 = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
        "/data/Beijing_2015_HourlyPM25_created20160201.csv").select("Year", "Month", "Day", "Hour",
                                                                    "Value", "QC Name")

    group_2017 = data_2017.withColumn("Grade", grade_function_udf(data_2017['Value'])).groupBy("Grade").count()
    group_2016 = data_2016.withColumn("Grade", grade_function_udf(data_2016['Value'])).groupBy("Grade").count()
    group_2015 = data_2015.withColumn("Grade", grade_function_udf(data_2015['Value'])).groupBy("Grade").count()

    group_2015.select("Grade", "count", group_2015['count'] / data_2015.count()).show()
    group_2016.select("Grade", "count", group_2016['count'] / data_2016.count()).show()
    group_2017.select("Grade", "count", group_2017['count'] / data_2017.count()).show()

    group_2017.select("Grade", "count").withColumn("percent", group_2017['count'] / data_2017.count()).show()
    spark.stop()
