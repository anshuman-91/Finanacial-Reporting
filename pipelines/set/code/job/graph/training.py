from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def training(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("order_category") == lit("Training")))
