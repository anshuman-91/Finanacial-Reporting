from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def balances_1(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "recipes_fabric":
        return spark.read\
            .format("parquet")\
            .schema(
              StructType([
                StructField("acc_id", IntegerType(), True), StructField("person_id", IntegerType(), True), StructField("product_id", IntegerType(), True), StructField("balance", DoubleType(), True), StructField("business_date", DateType(), True), StructField("import_ts", StringType(), True)
            ])
            )\
            .load("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/")
    else:
        raise Exception("No valid dataset present to read fabric")
