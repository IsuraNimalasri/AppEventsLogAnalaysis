"""
This Spark jobs reshape staging files into Data-warehouse table. Once reshape completed job writing data to database
"""

import datetime
import calendar

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


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

    # Read Staging Source
    stagingDF = spark.read.format('csv').option("header",True).load(STAGING_PATH)
    stagingDF.show(2)

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

    dashboardDimDF.show(5)

    # ----------------------------------
    # Device and Browser Dimension Table
    # ----------------------------------
    browserDimDF = stagingDF.select(
        col("browser").alias("b_name"),
        col("browser-platform").alias("b-os"),
        col("browser-version").alias("b_vrs"),
        concat( col("browser"),lit('='),col("browser-version")).alias("b_dimkey")
    ).dropDuplicates()

    browserDimDF.show(5)

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
    userDimDF.show(5)

    # ---------------------
    # Time Dimension Table
    # ---------------------
    timeDimDF = stagingDF.select(
        col("request_epoch").alias("time_dimkey"),
        col("request_timestamp").alias("req_ts"),
        col("request_date").alias("req_dt"),
        col("request_year").alias("req_y"),
        col("request_month").alias("req_m"),
        col("request_day").alias("req_d"),
        col("request_hour").alias("req_h"),
        col("request_dayofweek").alias("req_dow"),
        col("request_weekofyear").alias("req_woy")
    ).dropDuplicates()

    timeDimDF.show(5)

    # ==========================
    # Build Fact Tables
    # ==========================

    # ViewRequest  Fact Table
    ViewRequestFactDF = stagingDF.select(
        col("dashboard_id").alias("db_dimkey"),
        concat(col("browser"),lit('='), col("browser-version")).alias("b_dimkey"),
        col("request_epoch").alias("time_dimkey"),
        col("user").alias("user_dimkey"),
        col("user_event_value").alias("e_value")
    ).groupby(
        col("db_dimkey"),
        col("time_dimkey"),
        col("user_dimkey"),
        col("b_dimkey")
    ).agg(
        sum(col("e_value")).alias("total_requests")
    )

    ViewRequestFactDF.show()


