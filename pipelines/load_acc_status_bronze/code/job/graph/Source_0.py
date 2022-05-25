from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Source_0(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "recipes_fabric":
        return spark.read\
            .schema(
              StructType([
                StructField("_c0", StringType(), True), StructField("_c1", StringType(), True), StructField("_c2", StringType(), True), StructField("_c3", StringType(), True), StructField("_c4", StringType(), True)
            ])
            )\
            .option("header", False)\
            .option("sep", ",")\
            .option("recursiveFileLookup", True)\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/acc_status/")
    else:
        raise Exception("No valid dataset present to read fabric")
