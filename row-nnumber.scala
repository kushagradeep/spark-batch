from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
 df=spark.read.format("csv").option("inferSchema",true).option("header",true).load("file-1.csv")

df.show()
