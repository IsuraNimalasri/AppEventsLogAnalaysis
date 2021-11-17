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

if __name__ == '__main__':

    #  Declare Common Variables
    file_name = "event-view-dashboard.parquet"
    PROCESSING_PATH = f"../../s3_mimic/processing/{file_name}"
    STAGING_PATH = "../../s3_mimic/staging/{stage_name}"

    # Spark Job Entry point
    spark = SparkSession.builder.master("local[2]") \
        .appName('processing-to-staging') \
        .getOrCreate()

    # Register UDF
    segment_dashboards =udf(segment_dashboards)

    # Time Legacy Application
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    # Read Staging Source
    raw_stagingDF = spark.read.format('parquet')\
        .load(PROCESSING_PATH)

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


    # ----------------------------------
    # Device and Browser Dimension Table
    # ----------------------------------
    browserDimDF = stagingDF.select(
        col("browser").alias("b_name"),
        col("browser-platform").alias("b_os"),
        col("browser-version").alias("b_vrs"),
        concat( col("browser-platform"),lit('='),col("browser"),lit('='),col("browser-version")).alias("b_dimkey")
    ).dropDuplicates()


    #---------------------
    # User Dimension Table
    # --------------------
    userDimDF = stagingDF.select(
        col("user").alias("user_dimkey"),
        col("u_type"),
        split(col("user"),'@').getItem(0).alias("u_name"),
        col("user_domain").alias("u_domain")).dropDuplicates()

    userDimDF.show()

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

    # Create Staging_File


    try:
        for db_inf in df_list:
            db_table=db_inf[0]
            file_path = STAGING_PATH.format(stage_name=db_inf[1])
            db_table.write\
                .format("parquet")\
                .mode("overwrite")\
                .save(file_path)

            print("-----------------------")
            print(db_inf)
            print("-----------------------")

    except Exception as err:
        print(err)



#   --------------------------------------------------------------------------
#   TODO :
#       1. Need to create marker file or marker event with last read file folder.
#       2. Need to apply logger  instead of print messages
#       3. Need to add job statistics
#   ------------------------------------------------------------------------------





