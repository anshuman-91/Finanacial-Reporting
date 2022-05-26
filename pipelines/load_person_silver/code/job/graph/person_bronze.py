from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def person_bronze(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "recipes_fabric":
        return spark.read\
            .format("json")\
            .schema(
              StructType([
                StructField("addresses", ArrayType(
                StructType([
                  StructField("address_line1", StringType(), True), StructField("address_line2", StringType(), True), StructField("postal_code", StringType(), True), StructField("type", StringType(), True)
              ]), 
                True
              ), True), StructField("email", StringType(), True), StructField("id", StringType(), True), StructField("import_ts", TimestampType(), True), StructField("name", StringType(), True), StructField("updated_at", TimestampType(), True)
            ])
            )\
            .load("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/person/bronze/")
    else:
        raise Exception("No valid dataset present to read fabric")
