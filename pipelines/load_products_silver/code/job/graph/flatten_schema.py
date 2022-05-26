from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def flatten_schema(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("id"), 
        col("name"), 
        col("slug"), 
        col("properties.bonus_rate").alias("bonus_rate"), 
        col("properties.lock_in_period").alias("lock_in_period"), 
        col("import_ts")
    )
