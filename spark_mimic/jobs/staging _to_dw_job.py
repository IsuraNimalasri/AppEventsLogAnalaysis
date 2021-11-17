"""
This Spark jobs reshape staging files into Data-warehouse table. Once reshape completed job writing data to database
"""

import datetime
import calendar
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *


__author__ = "Isura Nimalasiri"
__version__ = "1.0.0"

def segment_dashboards(db_name):

    if "sale" in db_name:
        return "sales"
    elif "marketing" in db_name:
        return "marketing"
    elif "shipment" in db_name:
        return "shipment"
    elif "delivery" in db_name:
        return "delivery"
    elif "revenue" in db_name:
        return "finance"
    else:
        return "other"


def get_user_account_type(email):
    """
    This method identify email user type
    :param email:
    :return: persona or service_account
    """
    user_name = str(email).split('@')[0]
    if '.' in user_name:
        return "persona"
    else:
        return "service_account"



if __name__ == '__main__':
    #  Declare Common Variables
    DB_DRIVER_PATH = "../drivers/postgresql-42.3.1.jar"
    file_name = "event-view-dashboard.csv"
    STAGING_PATH = "../staging/{fname}".format(fname=file_name)

    # Spark Job Entry point
    spark = SparkSession.builder.master("local[2]") \
        .appName('dataprocesser-staging-to-dw') \
        .config("spark.jars", DB_DRIVER_PATH) \
        .getOrCreate()

    # Register UDF
    get_usertype = udf(get_user_account_type)
    segment_dashboards =udf(segment_dashboards)

    # Time Legacy Application
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    # Read Staging Source
    raw_stagingDF = spark.read.format('csv')\
        .option("header",True)\
        .load(STAGING_PATH)
    stagingDF = raw_stagingDF\
        .withColumnRenamed("request_timestamp","str_request_timestamp")\
        .withColumn("user_event_value",col("user_event_value").cast('int'))\
        .withColumn("request_timestamp",to_timestamp(col("str_request_timestamp"),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))\
        .dropDuplicates()

    stagingDF.printSchema()
    stagingDF.show(truncate=False)


    # ==========================
    # Build Dimension Tables
    # ==========================

    # --------------------------
    # Dashboard Dimension Table
    # -------------------------
    dashboardDimDF = stagingDF.select(
        col("dashboard_id").alias("db_dimkey"),
        col("dashboard_id").alias("db_name"),
        col("endpoint").alias("db_endpoint"),
        segment_dashboards(col("dashboard_id")).alias("db_type")
    ).dropDuplicates()

    # dashboardDimDF.show(5)

    # ----------------------------------
    # Device and Browser Dimension Table
    # ----------------------------------
    browserDimDF = stagingDF.select(
        col("browser").alias("b_name"),
        col("browser-platform").alias("b_os"),
        col("browser-version").alias("b_vrs"),
        concat( col("browser-platform"),lit('='),col("browser"),lit('='),col("browser-version")).alias("b_dimkey")
    ).dropDuplicates()

    # browserDimDF.show(5)

    #---------------------
    # User Dimension Table
    # --------------------
    userDimDF = stagingDF.select(
        col("user").alias("user_dimkey"),
        split(col("user"),'@').getItem(0).alias("u_name"),
        col("user_domain").alias("u_domain"),
        get_usertype(col("user")).alias("u_type")
    )\
        .withColumn("u_fname",split(col("u_name"),'[.]').getItem(0))\
        .withColumn("u_lname",split(col("u_name"),'[.]').getItem(1))\
        .dropDuplicates()
    # userDimDF.show(5)

    # ---------------------
    # Time Dimension Table
    # ---------------------
    timeDimDF = stagingDF.select(
        col("request_epoch").cast("long").alias("time_dimkey"),
        col("request_timestamp").alias("req_ts"),
        to_date(col("request_date")).alias("req_dt"),
        col("request_year").cast('int').alias("req_y"),
        col("request_month").cast('int').alias("req_m"),
        col("request_day").cast('int').alias("req_d"),
        col("request_hour").cast('int').alias("req_h"),
        col("request_dayofweek").cast('int').alias("req_dow"),
        col("request_weekofyear").cast('int').alias("req_woy")
    ).dropDuplicates()

    # timeDimDF.show(5)

    # ==========================
    # Build Fact Tables
    # ==========================

    #-------------------------
    # ViewRequests  Fact Table
    #-------------------------

    ViewRequestsFactDF = stagingDF.select(
        col("dashboard_id").alias("db_dimkey"),
        concat( col("browser-platform"),lit('='),col("browser"),lit('='),col("browser-version")).alias("b_dimkey"),
        col("request_epoch").cast("long").alias("time_dimkey"),
        col("user").alias("user_dimkey"),
        col("user_event_value").alias("e_value"),
        concat(col("request_epoch"),col("dashboard_id"),col("user"),col("browser"),lit('='),col("browser-version")).alias("vreq_factkey")
    ).groupby(
        col("vreq_factkey"),
        col("db_dimkey"),
        col("time_dimkey"),
        col("user_dimkey"),
        col("b_dimkey")
    ).agg(
        sum(col("e_value")).alias("num_of_requests")
    )

    # ViewRequestsFactDF.show()

    # -----------------------------
    # User Engagement  Fact Table
    # -----------------------------

    # Window Logic
    userNtime_windowSpec = Window.partitionBy("user").orderBy(desc(col("request_timestamp")))
    userRequestFlow_window = Window.partitionBy("user","request_date").orderBy(asc(col("request_timestamp")))

    userEngagementFactDF =stagingDF.select(
        # Dim Keys
        col("user").alias("user_dimkey"),
        col("dashboard_id").alias("db_dimkey"),
        concat( col("browser-platform"),lit('='),col("browser"),lit('='),col("browser-version")).alias("b_dimkey"),
        col("request_epoch").cast("long").alias("time_dimkey"),
        col("request_timestamp"),
        to_date(col("request_date")).alias("request_date"),
        col("user_event_value").cast("int").alias("user_event_value"),
        lead("request_timestamp", 1).over(userNtime_windowSpec).alias("last_request_timestamp"),
        rank().over(userRequestFlow_window).alias("viewflow")
    )\
        .withColumn("durationInSec",(col("request_timestamp").cast("long") - col("last_request_timestamp").cast('long')))\
        .withColumn("last_request_date",to_date(col("last_request_timestamp")))\
        .orderBy(desc("request_timestamp"))\
        .filter(col("last_request_timestamp").isNotNull()
                )

    userEngagementFactDF.show(truncate=False)

    # Write Datafame into DataWarehouse.

    df_list = [
        # (dashboardDimDF,"t_dim_dashboards"),
        # (browserDimDF,"t_dim_browser"),
        # (userDimDF,"t_dim_user"),
        # (timeDimDF,"t_dim_time"),
        # (ViewRequestsFactDF,"t_fact_view_reqs"),
        (userEngagementFactDF,"t_fact_user_eng")
    ]
    # try:
    #
    #     for db_inf in df_list:
    #
    #         db_table=db_inf[0]
    #         db_table.write\
    #             .format("jdbc")\
    #             .option("driver","org.postgresql.Driver")\
    #             .option("url", "jdbc:postgresql://localhost:5432/utracker") \
    #             .option( "user" ,"docker")\
    #             .option("password" , "docker")\
    #             .option("dbtable", f"public.{db_inf[1]}")\
    #             .mode("append")\
    #             .save()
    #         print("-----------------------")
    #         print(db_inf)
    #         print("-----------------------")
    #
    # except Exception as err:
    #     print(err)





    #  Top 10 High Trafic Dashboards (all the time)
    stagingDF.filter(col("status")=="200")\
        .groupby("dashboard_id")\
        .agg(
        sum("user_event_value").alias("View_Traffic")
        )\
        .orderBy(desc("View_Traffic"),desc("dashboard_id"))\
        .limit(10)\

    # Active user count over year
    stagingDF.groupby("request_year") \
        .agg(
        sum("user_event_value").alias("View_Traffic"),
        count_distinct("user").alias("active_user_count")
        ).withColumn("avg_traffic_made_by_a_user" ,(col("View_Traffic")/col("active_user_count"))) \
        .orderBy( asc("request_year")) \
        # .show(truncate=False)

#     popular dashboards
    stagingDF.groupby("dashboard_id") \
        .agg(
        count_distinct("user").alias("active_user_count"))\
        .orderBy(desc("active_user_count")) \
        .limit(5)\
        # .show(truncate=False)

    userEngagementFactDF.filter(col("request_date") == col("last_request_date"))\
    .groupby("db_dimkey")\
    .agg(
        sum("durationInSec").alias("Total_Duration"),
        count("request_date").alias("number_of_days"),
        count_distinct("user_dimkey").alias("number_of_users")
    ).withColumn("avg_engagement_time",col("Total_Duration")/(col("number_of_users")*col("number_of_days")))\
    .orderBy(desc("avg_engagement_time"))

    # Dashboard Audience

    stagingDF.groupby("user_domain").agg(
        count_distinct("user").alias("number_of_users"),
        sum("user_event_value").alias("total_requests"))\
    # .orderBy(desc("number_of_users")).show(truncate=False)

    # Dashboard Access Browser and
    stagingDF.groupby("browser-platform").agg(
        count_distinct("user").alias("number_of_users"),
        sum("user_event_value").alias("total_requests"))\
        .orderBy(desc("number_of_users"))

    # marketing - overview

    stagingDF.filter(col("dashboard_id")=="marketing-overview").groupby("request_month")\
        .agg(
        count_distinct("user").alias("number_of_users"),
        count_distinct("user_domain").alias("doamin"),
        sum("user_event_value").alias("request_count")
    ).orderBy(desc("request_count"))










