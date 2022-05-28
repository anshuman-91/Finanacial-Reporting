from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_acc_status_bronze = acc_status_bronze(spark)
    df_null_check = null_check(spark, df_acc_status_bronze)
    df_dedup = dedup(spark, df_null_check)
    df_dvr = dvr(spark, df_dedup)
    df_milestone_keys = milestone_keys(spark, df_dvr)
    acc_status_silver(spark, df_milestone_keys)

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
