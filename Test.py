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
    .load("data/202*/*/*/mara-log/requests-*.json")

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
    col("path"),
    split(col("user_domain"),'[.]').getItem(0).alias("team"),
    split(col("user_domain"),'[.]').getItem(1).alias("territory")
)

print("-------------------- User Engagemnet------------------")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
windowSpec  = Window.partitionBy("active_user").orderBy(desc(col("log_ts")))

tem_tab = t_dashboard_useage.select(
    col("log_ts"),
    col("view_args_id").alias("dashboard_name"),
    col("active_user"),
    col("team").alias("user_company"),
    col("territory").alias("company_territory"),
    col("browser-platform"),
    col("browser"),
    col("event_count"),
    col("log_date")
)

tem_tab.select(
    col('*'),
    rank().over(windowSpec).alias("rank"),
    lead("log_ts", 1).over(windowSpec).alias("lastViewedTime"),
    to_date(lead("log_ts", 1).over(windowSpec),"yyyy-MM-dd").alias("lastViewedDate"))\
    .where(col("lastViewedTime").isNotNull() & (col("lastViewedDate")== col("log_date")) )\
    .withColumn("durationInSec",(col("log_ts").cast("long") - col("lastViewedTime").cast("long")))

t_dashboard_useage.withColumnRenamed("view_args_id","dhasboard_name")\
    .withColumn("rank", rank().over(windowSpec)) \
    .withColumn("lastViewedTime", lead("log_ts", 1).over(windowSpec))\
    .withColumn("lastViewedDate",to_date(col("lastViewedTime"),"yyyy-MM-dd").alias("log_date"))\
    .where(col("lastViewedTime").isNotNull() & (col("lastViewedDate")== col("log_date")) )\
    .withColumn("durationInSec",(col("log_ts").cast("long") - col("lastViewedTime").cast("long")))\
    .write.format("csv").option("header",True).mode('overwrite').save("landing/user_engagement.csv")

# tem_tab.show(1)

# t_dashboard_useage.write.csv("landing/user_engagement.csv")

t_dashboard_useage.groupby(col('active_user')).agg(
    max(col("log_ts")).alias("LastViewTime"),
    datediff(current_timestamp() , max(col("log_ts")).alias("LastViewTime") ).alias("age")
).orderBy("LastViewTime")

t_dashboard_useage.show(2)

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

total_v_count.write.format("csv").option("header",True).mode('overwrite').save('landing/total_v_count.csv')

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

#
# requestLogDF.select(
#         col("view-args").getField("column_name").alias("column_name"),
#         col("view-args").getField("cube_name").alias("cube_name"),
#         col("view-args").getField("data_set_id").alias("data_set_id"),
#         col("view-args").getField("db_alias").alias("db_alias"),
#         col("view-args").getField("email").alias("email"),
#         col("view-args").getField("filename").alias("filename"),
#         col("view-args").getField("filter_pos").alias("filter_pos"),
#         col("view-args").getField("limit").alias("limit"),
#         col("view-args").getField("new_role").alias("new_role"),
#         col("view-args").getField("params").alias("params"),
#         col("view-args").getField("path").alias("path"),
#         col("view-args").getField("pos").alias("pos"),
#         col("view-args").getField("query_id").alias("query_id"),
#         col("view-args").getField("run_id").alias("run_id"),
#         col("view-args").getField("schemas").alias("schemas"),
#         col("view-args").getField("sort_col").alias("sort_col"),
#         col("view-args").getField("sort_dir").alias("sort_dir"),
#         col("view-args").getField("table_name").alias("table_name"),
#         col("view-args").getField("term").alias("term")
#     ).filter(col("endpoint") == "dashboards.view_dashboard").count()
