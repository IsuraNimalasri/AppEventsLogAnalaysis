"""
This module read jobs from raw location, then  standardized data and reshape to table structure.
"""

import datetime
import calendar

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

__author__ = "Isura Nimalasiri"
__version__ = "1.0.0"

if __name__ == '__main__':
    #  Declare Common Variables
    LANDING_PATH = "../data/*/*/*/mara-log/requests-*.json"
    STAGING_PATH = "../staging/{fname}"


    # Create Spark Entry-Point
    spark = SparkSession \
        .builder.master("local[2]") \
        .appName('dataprocesser-landing-to-staging') \
        .getOrCreate()

    # Read and create dataframe
    requestLogDF = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .load(LANDING_PATH)

    # Read request-logs about view_dashboard endpoint
    viewDashboardDF = requestLogDF \
        .select(
        # Dashboard Information
            col("view-args").getField("id").alias("dashboard_id"),
            col("endpoint"),
            col("status"),
            col("method"),

        # Browser and device Information
            col("browser"),
            col("browser-platform"),
            col("browser-version"),

        # Time Information
            (to_timestamp(col("timestamp")).cast('double') * 1000).cast('long').alias("request_epoch"),
            to_timestamp(col("timestamp")).alias("request_timestamp"),
            to_date(col("timestamp")).alias("request_date"),
            year(col("timestamp")).alias("request_year"),
            month(col("timestamp")).alias("request_month"),
            dayofmonth(col("timestamp")).alias("request_day"),
            dayofweek(col("timestamp")).alias("request_dayofweek"),
            weekofyear(col("timestamp")).alias("request_weekofyear"),
            hour(col("timestamp")).alias("request_hour"),

        # User Information
            col("user"),
            split(col("user"), '@').getItem(1).alias("user_domain"),
            lit(1).alias("user_event_value")) \
        .filter(
        (col("endpoint") == "dashboards.view_dashboard")
        &
        col("dashboard_id").isNotNull()
    )

    #  Write data into staging Location
    file_name = "event-view-dashboard.csv"
    viewDashboardDF.write.format("csv").option("header",True).mode('overwrite').save(STAGING_PATH.format(fname=file_name))

#     -----------------------------------------------------------------------
#     TODO :
#       1. Need new code line to move processed data from landing to archive.
#       2. Need to create marker file or marker event with process statistics.
#     _------------------------------------------------------------------------
#
