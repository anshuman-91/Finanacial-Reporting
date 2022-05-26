from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def add_row_num2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "row_num",
        row_number().over(Window.partitionBy(col("acc_id"), col("business_date")).orderBy(col("import_ts").desc()))
    )
