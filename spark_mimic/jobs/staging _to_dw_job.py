"""
This Spark jobs read data from staging files load into Data-warehouse table.
"""

import datetime
import calendar
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

from pipeline_util.basic_utils import read_config

__author__ = "Isura Nimalasiri"
__version__ = "1.0.0"

if __name__ == '__main__':
    #  Declare Common Variables
    DB_DRIVER_PATH = "../drivers/postgresql-42.3.1.jar"
    STAGING_PATH = "../../s3_mimic/staging/{stage_name}"
    CONF_PATH = "../../pipeline-meta/cfg_event-view-dashboard.json"

    # Read Config
    conf_json = read_config(CONF_PATH)


    # Spark Job Entry point
    spark = SparkSession.builder.master("local[2]") \
        .appName('staging-to-dw') \
        .config("spark.jars", DB_DRIVER_PATH) \
        .getOrCreate()

    for db_table in conf_json['staging_args']['db_tables_info']:
        #  Table_Name
        print( db_table['tab'])

        #  Create temp_data frame
        stage_db_tableDF = spark.read.format('parquet').parquet(STAGING_PATH.format(
            stage_name=db_table['tab']
        ))
        stage_db_tableDF.show(truncate=False)

        if "NA" != db_table['sdc']:
            dw_dim_DF = spark.read.format('jdbc') \
                        .option("driver","org.postgresql.Driver")\
                        .option("url", "jdbc:postgresql://localhost:5432/utracker") \
                        .option( "user" ,"docker")\
                        .option("password" , "docker")\
                        .option("dbtable", f"public.{db_table['tab']}")\
                        .load()

    #  ------------------------------------------------------------------------------------
    #  Todo
    #    1.Join with staging fileDF and particular dw_dim_DF
    #    2. Identifying new data insert with 'Y" flag
    #    3. Identifying update data rows insert with 'Y" flag and old data update with "N"
    # --------------------------------------------------------------------------------------

        try:
            stage_db_tableDF.write\
                    .format("jdbc")\
                    .option("driver","org.postgresql.Driver")\
                    .option("url", "jdbc:postgresql://localhost:5432/utracker") \
                    .option( "user" ,"docker")\
                    .option("password" , "docker")\
                    .option("dbtable", f"public.{db_table['tab']}")\
                    .mode("append")\
                .save()
            print("-----------------------")
            print(db_table['tab'])
            print("-----------------------")

        except Exception as err:
            print(err)













