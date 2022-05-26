from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "balance_change",
        when(col("prev_balance").isNotNull(), col("balance") - col("prev_balance")).otherwise(col("balance"))
    )
