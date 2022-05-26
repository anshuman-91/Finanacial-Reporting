from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from . import *

def latest(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_add_row_num = add_row_num(spark, in0)
    df_row_num_eq_1 = row_num_eq_1(spark, df_add_row_num)
    df_drop_row_num = drop_row_num(spark, df_row_num_eq_1)

    return df_drop_row_num
