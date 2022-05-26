from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def transactions_bronze(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "recipes_fabric":
        return spark.read\
            .format("parquet")\
            .schema(
              StructType([
                StructField("acc_id", IntegerType(), True), StructField("tran_id", StringType(), True), StructField("business_date", DateType(), True), StructField("tran_amount", DoubleType(), True), StructField("tran_type", StringType(), True), StructField("tran_ts", TimestampType(), True), StructField("import_ts", TimestampType(), True)
            ])
            )\
            .load("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/transactions/bronze/")
    else:
        raise Exception("No valid dataset present to read fabric")
