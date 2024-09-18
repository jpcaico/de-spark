#import libraries and init spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print(spark)

# load data
df_device = spark.read.json("/Users/jalvi/Desktop/jp-repos/de-spark/docs/files/device/device_2022_6_7_19_39_24.json")
df_device.show()

#schema
df_device.printSchema()

#columns
print(df_device.columns)

# rows
print(df_device.count())

# select columns
df_device.select("manufacturer", "model", "platform").show()
df_device.selectExpr("manufacturer", "model", "platform as type").show()

#filter
df_device.filter(df_device.manufacturer == "Xiamomi").show()

# group by
df_device.groupBy("manufacturer").count().show()

df_grouped_manufacturer = df_device.groupBy("manufacturer").count()
df_grouped_manufacturer.show()

# read all files
df_all = spark.read.json("/Users/jalvi/Desktop/jp-repos/de-spark/docs/files/device/*.json")
print(df_all.count())