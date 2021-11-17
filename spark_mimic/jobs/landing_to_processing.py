"""
This module read jobs from raw location, then  standardized data and reshape to table structure.
"""

import datetime
import calendar
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pipeline_util.basic_utils import read_config


__author__ = "Isura Nimalasiri"
__version__ = "1.0.0"

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
    LANDING_PATH = "../../s3_mimic/landing/data/2021/01/0*/mara-log/requests-*.json"
    PROCESSING_PATH = "../../s3_mimic/processing/{fname}"
    pipeline_name = "event-view-dashboard"

    # Read Config
    conf_json = read_config(pipeline_name)

    # Create Spark Entry-Point
    spark = SparkSession \
        .builder.master("local[2]") \
        .appName('landing-to-processing') \
        .getOrCreate()

    # Read and create dataframe
    requestLogDF = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .load(LANDING_PATH)

    # Register UDF
    get_usertype = udf(get_user_account_type)

    # Read request-logs about view_dashboard endpoint
    logFilteredDF = requestLogDF \
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
            sha1(col("user")).alias("user"), # encrypt
            get_usertype(col("user")).alias("u_type"),
            split(col("user"), '@').getItem(1).alias("user_domain"),
            lit(1).alias("user_event_value")) \
        .filter(
        (col("endpoint") == f"{conf_json['landing_args']['endpoint_Filter']}")
        &
        col("dashboard_id").isNotNull()
    )

    #  Write data into staging Location
    file_name = "event-view-dashboard.parquet"
    try:
        logFilteredDF.write.format("parquet").mode('overwrite').save(PROCESSING_PATH.format(fname=file_name))

    except Exception as err:
        print(err)

#     -----------------------------------------------------------------------
#     TODO :
#       1. Need new code line to move read data from landing to archive.
#       2. Need to create marker file or marker event with last reading folder.
#       3. Need to apply logger  instead of print messages
#       4. Need to add job statistics
# #     _------------------------------------------------------------------------
#
