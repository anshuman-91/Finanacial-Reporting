from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def signed_tran_amt_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "signed_tran_amt",
        when((col("tran_type") == lit("DEBIT")), - col("tran_amount")).otherwise(col("tran_amount"))
    )
