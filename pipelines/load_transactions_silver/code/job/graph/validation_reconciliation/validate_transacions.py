from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def validate_transacions(spark: SparkSession, in0: DataFrame) -> DataFrame:

    if (in0.filter(col("tran_amount") < 0).count() > 0):
        raise Exception("Found negative account balances")
    elif (in0.filter(~ col("tran_type").isin(["INTEREST", "DEBIT", "CREDIT"])).count() > 0):
        raise Exception("Found invalid Transaction types. Allowed values = [\"INTEREST\", \"DEBIT\", \"CREDIT\"]")

    out0 = in0

    return out0
