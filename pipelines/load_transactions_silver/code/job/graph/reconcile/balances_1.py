from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def balances_1(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "recipes_fabric":
        return spark.read\
            .format("parquet")\
            .load("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/")
    else:
        raise Exception("No valid dataset present to read fabric")
