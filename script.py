import os
import shutil

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import re

'''
Pipeline to read, window and process the logfile
'''


def extract_gameid_modid_udf():
    # Nested JSON
    def inner(path):
        match = re.search(r"/v1/games/(\d+)/mods/(\d+)/", path)
        return (match.group(1), match.group(2)) if match else (None, None)

    return F.udf(inner,
                 StructType(
                     [
                         StructField("gameid", StringType(), True),
                         StructField("modid", StringType(), True)
                     ]
                 )
                 )


# Create spark session object
def create_spark_session(app_name, master):
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


# Data process funtion
def process_mod_downloads(spark, input_path, output_path):
    extract_id_udf = extract_gameid_modid_udf()
    spark.udf.register("extract_gameid_modid", extract_id_udf)

    log_schema = StructType([
        StructField("ClientRequestPath", StringType(), True),
        StructField("ClientIP", StringType(), True),
        StructField("EdgeStartTimestamp", TimestampType(), True)
    ])

    log_df = spark.read.schema(log_schema).json(input_path).repartition("ClientRequestPath")

    downloads_df = log_df \
        .withColumn("ids", extract_id_udf("ClientRequestPath")) \
        .select("ClientIP", "ids.*", F.col("EdgeStartTimestamp").alias("timestamp")) \
        .filter(F.col("gameid").isNotNull() & F.col("modid").isNotNull())

    windowed_downloads = downloads_df \
        .withWatermark("timestamp", "24 hours") \
        .dropDuplicates(["ClientIP", "gameid", "modid", "timestamp"]) \
        .groupBy(F.window("timestamp", "24 hours"), "gameid", "modid").count()

    windowed_downloads.write.format("delta").mode("append").save(output_path)


# Clean up input
def clean_up():
    # Check if the directory exists
    if os.path.exists(input_path) and os.path.isdir(input_path):

        # List all files and directories in the input_path
        for filename in os.listdir(input_path):
            file_path = os.path.join(input_path, filename)

            # Check if it's a file or a directory
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # Remove the file or link
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Remove the directory and all its contents

        print(f"All files in '{input_path}' have been deleted.")
    else:
        print(f"The directory '{input_path}' does not exist.")


# Trigger the pipeline
if __name__ == "__main__":
    app_name = "Mod_Download_ETL_Local"
    master = "local[*]"
    input_path = "./input"  # Suspect that this is the input path
    output_path = "./output"  # Suspect that this is the output path

    spark = create_spark_session(app_name, master)
    try:
        process_mod_downloads(spark, input_path, output_path)
    except Exception as e:
        print(e)
    finally:
        spark.stop()
        clean_up()
