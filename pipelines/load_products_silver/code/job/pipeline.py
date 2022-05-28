from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_products_bronze = products_bronze(spark)
    df_latest = latest(spark, df_products_bronze)
    df_flatten_schema = flatten_schema(spark, df_latest)
    df_null_check = null_check(spark, df_flatten_schema)
    df_milestone_keys = milestone_keys(spark, df_null_check)
    df_validate_bonus_rate = validate_bonus_rate(spark, df_milestone_keys)
    products_silver(spark, df_validate_bonus_rate)

def main():
    Utils.initializeFromArgs(Utils.parseArgs())
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    initialize(spark)
    pipeline(spark)

if __name__ == "__main__":
    main()
