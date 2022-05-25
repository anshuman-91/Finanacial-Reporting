from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_person_external = person_external(spark)
    df_import_ts = import_ts(spark, df_person_external)
    person_bronze(spark, df_import_ts)

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
