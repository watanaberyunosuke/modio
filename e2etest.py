import unittest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import Row
from script import extract_gameid_modid_udf, create_spark_session, process_mod_downloads, clean_up

'''
This is a test script to run an E2E Unit Test for the pipeline script 
'''


class SparkETLTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test_App") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_extract_gameid_modid_udf(self):
        # Testing the UDF
        test_udf = extract_gameid_modid_udf()
        test_data = [
            ("/v1/games/123/mods/456/", "123", "456"),
            ("/v1/games/789/mods/101/", "789", "101"),
            ("/invalid/path/", None, None)
        ]

        for path, expected_gameid, expected_modid in test_data:
            df = self.spark.createDataFrame([(path,)], ["path"])
            result = df.withColumn("result", test_udf(F.col("path"))).select("result.*").first()
            self.assertEqual(result.gameid, expected_gameid)
            self.assertEqual(result.modid, expected_modid)

    def test_process_mod_downloads(self):
        # Testing the process_mod_downloads function
        # This test should create a mock input data set and check the output of the function
        pass  # Implement based on your specific logic and requirements


# Running the tests
if __name__ == '__main__':
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
