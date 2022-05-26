from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def WindowFunction_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "prev_balance",
        lag(col("balance"), 1).over(Window.partitionBy(col("acc_id")).orderBy(col("business_date").desc()))
    )
