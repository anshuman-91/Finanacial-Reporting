from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_all_orders = all_orders(spark)
    df_research = research(spark, df_all_orders)
    df_Limit_1 = Limit_1(spark, df_research)
    df_all_orders_1 = all_orders_1(spark)
    df_training = training(spark, df_all_orders_1)
    df_Limit_2 = Limit_2(spark, df_training)
    df_union = union(spark, df_Limit_1, df_Limit_2)
    total_orders(spark, df_union)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, Utils.parseArgs())
    initialize(spark)
    pipeline(spark)

if __name__ == "__main__":
    main()
