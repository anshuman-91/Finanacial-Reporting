from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def sum_trans(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("acc_id"), col("business_date"))

    return df1.agg(sum(col("signed_tran_amt")).alias("signed_tran_amount"))
