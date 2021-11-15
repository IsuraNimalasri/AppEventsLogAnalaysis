"""
POC to Basic Investigation of a Datasource
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
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
    .load("data/2020/1*/*/mara-log/requests-*.json")

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
# dbviewDF.groupby(col("view_args_id")).count()
print('---------- User Table ------------------')
user_tab = dbviewDF.select(
    col("user").alias("user_id"),
    col("user").alias("email"),
    split(col("user"),'[.]').name("user_infor"),
)

user_infor = """
email        : antonia.lopiccolo@lampenwelt.de
first_name   : Antonia
last_name    : Lopiccolo
Team         : lampenwelt
RegistoredAt : de
user_id      : antonia.lopiccolo@lampenwelt.de
"""
print(user_infor)
# user_tab.show(10)

dbviewDF



# dbviewDF.show(10)

# print("------- Dashabord Useage ------------------")
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

print("-------------------- User Engagemnet------------------")

windowSpec  = Window.partitionBy("active_user").orderBy(desc(col("log_ts")))

t_dashboard_useage.withColumn("rank", rank().over(windowSpec)) \
      .withColumn("lastViewedTime", lead("log_ts", 1).over(windowSpec))\
      .withColumn("lastViewedDate",to_date(col("lastViewedTime"),"yyyy-MM-dd").alias("log_date"))\
      .where(col("lastViewedTime").isNotNull() & (col("lastViewedDate")== col("log_date")) )\
    .withColumn("durationInSec",(col("log_ts").cast("long") - col("lastViewedTime").cast("long")))


t_dashboard_useage.groupby(col('active_user')).agg(
    max(col("log_ts")).alias("LastViewTime"),
    datediff(current_timestamp() , max(col("log_ts")).alias("LastViewTime") ).alias("age")
).orderBy("LastViewTime")
# t_dashboard_useage

print("-------------------- User Engagemnet END------------------")

# db_table = t_dashboard_useage.groupby(col("view_args_id"),col("log_date"),col("territory"),col('team'))\
#     .agg(
#     sum("event_count").alias("activity_count"),
#     count_distinct("active_user").alias("active_users")
# )

# db_table.show()



# need to encrypt
# db_table.write\
#     .format("jdbc")\
#     .option("driver","org.postgresql.Driver")\
#     .option("url", "jdbc:postgresql://localhost:5432/utracker") \
#     .option( "user" ,"docker")\
#     .option("password" , "docker")\
#     .option("dbtable", "public.dashboard_usage")\
#     .mode("overwrite")\
#     .save()

print("---------------- Traffic to Views ---------------------")
# dbviewDF.printSchema()
#
t_traficOnDashboard = dbviewDF.select(
    col("view_args_id").alias("dashboard_name"),
    col("user"),
    to_timestamp(col("timestamp")).cast("long").alias("time_in_sec"),
    to_date(col("timestamp")).alias("view_date"),
    col("method"),
    lit(1).alias("event_value")
)

# Total View Trafic for all dashboards

total_v_count = t_traficOnDashboard\
    .groupby(col("view_date"),col("dashboard_name")).agg(
     sum("event_value").alias("total_views")
)

# total_v_count.show()

total_v_count.write.csv('landing/total_v_count.csv')

# total_v_count.groupby("dashboard_name").agg(
#     (sum("total_views") / count("view_date")).alias("avg_views_per_day"),
#
# ).orderBy(desc("avg_views_per_day"))
#



print("---------------- Traffic to Views END ---------------------")

print("---------------- Browser Info ---------------------")
dbviewDF.select(
    col("browser"),
    col("browser-version")
    ).dropDuplicates().orderBy(asc("browser"),desc("browser-version"))\
    .show()