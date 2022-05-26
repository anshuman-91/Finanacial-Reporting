from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def row_num_eq1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("row_num") == lit(1)))
