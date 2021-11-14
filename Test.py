"""
POC to Basic Investigation of a Datasource
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]") \
                    .appName('Unmind-User-Request') \
    .config("spark.jars", "drivers/postgresql-42.3.1.jar") \
                    .getOrCreate()

# df = spark.read.format('org.apache.spark.sql.json') \
#         .load("data/2021/01/*/mara-log/requests-*.json")

log_generic_schema = StructType([
    StructField("user",StringType(),False),
    StructField("endpoint",StringType(),False),
    StructField("method",StringType(),False),
    StructField("browser",StringType(),False),
    StructField("browser-platform",StringType(),False),
    StructField("browser-version",StringType(),False),
    StructField("path",StringType(),False),
    StructField("view-args",StructType(),False),
    StructField("timestamp",StringType(),False),
 ])

log_df =  spark.read\
    .format("org.apache.spark.sql.json")\
    .option("inferSchema","true")\
    .load("data/*/*/*/mara-log/requests-*.json")

log_df.show(2)
#
# df.show(3)
# df.count()
# df.groupby(col("view-args")).count().show()

dbviewDF = log_df.select(
    col("*"),
    col("view-args").getField("id").alias("view_args_id"),
    split(col("user"), '@').getItem(1).alias("user_domain"))\
    .filter(col("endpoint") == "dashboards.view_dashboard")\
    .filter(col("view_args_id").isNotNull())

print('---------- Dashboard Count ------------------')
dbviewDF.groupby(col("view_args_id")).count()
print('---------- User Count ------------------')
dbviewDF.select(count_distinct(col("user")))

# dbviewDF.show(10)

print("------- Dashabord Useage ------------------")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
t_dashboard_useage = dbviewDF.select(
    col("view_args_id"),
    col("user_domain"),
    col("user").alias("active_user"),
    to_timestamp(col("timestamp")).alias("log_ts"),
    to_date(col("timestamp"),"yyyy-MM-dd").alias("log_date"),
    expr("EXTRACT(HOUR FROM timestamp)").alias("log_hour"),
    lit(1).alias("event_count"),
    col("browser"),
    col("browser-platform"),
    split(col("path"),'/').alias("view_path"),
    split(col("user_domain"),'[.]').getItem(0).alias("team"),
    split(col("user_domain"),'[.]').getItem(1).alias("territory")

)

# t_dashboard_useage.show(10)

db_table = t_dashboard_useage.groupby(col("view_args_id"),col("log_date"),col("territory"),col('team'))\
    .agg(
    sum("event_count").alias("activity_count"),
    count_distinct("active_user").alias("active_users")
)

db_table.show()

# need to encrypt
db_table.write\
    .format("jdbc")\
    .option("driver","org.postgresql.Driver")\
    .option("url", "jdbc:postgresql://localhost:5432/utracker") \
    .option( "user" ,"docker")\
    .option("password" , "docker")\
    .option("dbtable", "public.dashboard_usage")\
    .mode("overwrite")\
    .save()

# log_df.printSchema()