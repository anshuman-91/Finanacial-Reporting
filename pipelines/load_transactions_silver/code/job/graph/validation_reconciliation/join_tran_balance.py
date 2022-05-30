from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def join_tran_balance(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.acc_id") == col("in1.acc_id")) & (col("in0.business_date") == col("in1.business_date"))),
          "inner"
        )\
        .select(col("in0.acc_id").alias("acc_id"), col("in0.business_date").alias("business_date"), col("in0.signed_tran_amount").alias("signed_tran_amount"), col("in1.balance_change").alias("bal_change"))
