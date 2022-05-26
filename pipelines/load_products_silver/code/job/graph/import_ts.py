from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def import_ts(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("business_date", current_date())