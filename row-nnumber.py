from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions  import spark_partition_id
from pyspark.sql.functions import rank
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast	

spark = SparkSession.builder.appName("Row Number").config("spark.sql.shuffle.partitions","5").config("spark.master","local[1]").config("spark.default.parallelism","5").getOrCreate()
df=spark.read.format("csv").option("inferSchema","true").option("header","true").load("file:///Users/kushagradeep/mobileum/spark-2.4.3-bin-hadoop2.7/bin/scala-learning-data/file-1.csv")
partDf=df.orderBy("Age").withColumn("partitionId", spark_partition_id())

window=Window.partitionBy("partitionId").orderBy("Age")

rankDf = partDf.withColumn("local_rank", rank().over(window))

rankDf.show(100)
	
# Creating static dataset
tempDf=rankDf.groupBy("partitionId").agg(F.max("local_rank").alias("max_rank"))
window2=Window.orderBy("partitionId").rowsBetween(Window.unboundedPreceding,Window.currentRow)

statsDf=tempDf.withColumn("cum_rank", F.sum("max_rank").over(window2))

statsDf.show(100)

joinDf=statsDf.alias("l").join(statsDf.alias("r"), F.col("l.partitionId") == F.col("r.partitionId")+1, "left").select(F.col("l.partitionId"), F.coalesce(F.col("r.cum_rank"),F.lit(0)).alias("sum_factor"))
 	
finalDf=rankDf.join(F.broadcast(joinDf),rankDf.partitionId==joinDf.partitionId).withColumn("rank", rankDf.local_rank + joinDf.sum_factor).orderBy("rank")

#print("Number of partitions: {}".format(df3.rdd.getNumPartitions()))

finalDf.explain(True)
finalDf.show(100)
configurations = spark.sparkContext.getConf().getAll()
for conf in configurations:
  print(conf)
