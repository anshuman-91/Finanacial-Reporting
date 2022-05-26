from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def business_date(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("business_date", to_date(lit("2022-05-05"), "yyyy-MM-dd"))
