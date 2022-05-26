from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def null_check(spark: SparkSession, in0: DataFrame) -> DataFrame:

    if (
        in0.filter(
    col("acc_id").isNull()
    | col("tran_amount").isNull()
    | col("tran_type").isNull()
    | col("tran_id").isNull() 
    | col("business_date").isNull()
    | col("tran_ts").isNull()
    ).count()
        > 0
    ):
        raise Exception("Schema Validation Failed")

    out0 = in0

    return out0
