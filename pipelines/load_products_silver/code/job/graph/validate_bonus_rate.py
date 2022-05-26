from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def validate_bonus_rate(spark: SparkSession, in0: DataFrame) -> DataFrame:

    if (in0.filter(col("bonus_rate") < 0).count() > 0):
        raise Exception("Found negative bonus rate")

    out0 = in0

    return out0
