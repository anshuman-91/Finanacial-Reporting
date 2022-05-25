from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    (df_Generator_acc_status_first_df,  df_Generator_acc_status_second_df,  df_Generator_transactions_df,      df_Generator_people_df,  df_Generator_products_df) = Generator(
        spark
    )
    acc_20220505_csv_1(spark, df_Generator_acc_status_second_df)
    df_Reformat_1_3 = Reformat_1_3(spark, df_Generator_people_df)
    acc_20220504_csv(spark, df_Generator_acc_status_first_df)
    df_Reformat_1_4 = Reformat_1_4(spark, df_Generator_products_df)
    df_Reformat_1_2 = Reformat_1_2(spark, df_Generator_transactions_df)

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
