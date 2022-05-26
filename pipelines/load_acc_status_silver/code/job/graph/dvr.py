from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def dvr(spark: SparkSession, in0: DataFrame) -> DataFrame:

    if (in0.filter(col("balance") < 0).count() > 0):
        raise Exception("Found negative account balances")

    out0 = in0

    return out0
