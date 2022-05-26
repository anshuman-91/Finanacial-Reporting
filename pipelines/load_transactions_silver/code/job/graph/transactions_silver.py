from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def transactions_silver(spark: SparkSession, in0: DataFrame):
    if Config.fabricName == "recipes_fabric":
        in0.write\
            .option("header", True)\
            .option("sep", ",")\
            .mode("error")\
            .option("separator", ",")\
            .option("header", True)\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver")
    else:
        raise Exception("No valid dataset present to read fabric")
