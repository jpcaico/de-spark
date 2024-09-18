from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession \
.builder \
.appName("app-gcs-json") \
.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "path_to_credential/credential.json") \
.getOrCreate()

print(SparkConf().getAll())
spark.sparkContext.setLogLevel("INFO")

df_users = spark.read.json("gs://bucket-name/users/*.json")
df_users.show()
