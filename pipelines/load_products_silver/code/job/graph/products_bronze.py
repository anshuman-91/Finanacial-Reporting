from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def products_bronze(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "recipes_fabric":
        return spark.read\
            .format("avro")\
            .schema(
              StructType([
                StructField("id", StringType(), True), StructField("name", StringType(), True), StructField("properties", StructType([
                  StructField("bonus_rate", DoubleType(), True), StructField("lock_in_period", IntegerType(), True)
                ]), True), StructField("slug", StringType(), True), StructField("updated_at", TimestampType(), True)
            ])
            )\
            .load("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/products/bronze/")
    else:
        raise Exception("No valid dataset present to read fabric")
