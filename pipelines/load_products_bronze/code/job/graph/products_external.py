from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def products_external(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "recipes_fabric":
        return spark.read\
            .format("avro")\
            .option("recursiveFileLookup", True)\
            .schema(
              StructType([
                StructField("id", StringType(), True), StructField("name", StringType(), True), StructField("properties", ArrayType(StructType([]), True), True), StructField("slug", StringType(), True), StructField("updated_at", TimestampType(), True)
            ])
            )\
            .load("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/products")
    else:
        raise Exception("No valid dataset present to read fabric")
