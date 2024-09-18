import os
from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession \
    .builder \
    .appName("app-snowflake") \
    .getOrCreate()

print(SparkConf().getAll())
spark.sparkContext.setLogLevel("INFO")

SNOWFLAKE_OPTIONS = {
    'sfURL': os.environ.get("SNOWFLAKE_URL", "SNOWFLAKE_URL"),
    'sfAccount': os.environ.get("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_ACCOUNT"),
    'sfUser': os.environ.get("SNOWFLAKE_USER", "SNOWFLAKE_USER"),
    'sfPassword': os.environ.get("SNOWFLAKE_PASSWORD", "SNOWFLAKE_PASSWORD!"),
    'sfDatabase': os.environ.get("SNOWFLAKE_DATABASE", "SNOWFLAKE_DATABASE"),
    'sfSchema': os.environ.get("SNOWFLAKE_SCHEMA", "SNOWFLAKE_SCHEMA"),
    'sfWarehouse': os.environ.get("SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_WAREHOUSE"),
    'sfRole':  os.environ.get("SNOWFLAKE_ROLE", "SNOWFLAKE_ROLE")
    }

# query pushdown {}
df_subscription = spark.read.format("snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .option('dbtable', "Subscription") \
    .load()

df_subscription.show()