from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import min, max, avg

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('json').option("multiline","true").load("D:/01.Develop/02.PYTHON/04.HomePrice/srcFile/*")
df = df.withColumn("housePc", col("housePc").cast("bigint"))

AvgHome = df.groupBy("ldCode", "ldCodeNm")
result = AvgHome.agg(min(df.housePc).alias("minPrice"), max(df.housePc).alias("maxPrice"), avg(df.housePc).cast("bigint").alias("avgPrice"))

# json File Write
# AvgHome.orderBy(col("avg(housePc)").asc()).select("*").write.format("json").mode("overwrite").save("D:/result.json")
result.orderBy(col("avgPrice")).select("*").write.format("json").mode("overwrite").save("D:/result.json")

# csv File Write
#AvgHome.orderBy(col("avg(housePc)").asc()).select("*").write.format("csv").mode("overwrite").save("D:/result.csv")
result.orderBy(col("avgPrice")).select("*").write.format("csv").mode("overwrite").save("D:/result.csv")