from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from . import *

def reconcile(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_signed_tran_amt_1 = signed_tran_amt_1(spark, in0)
    df_sum_trans = sum_trans(spark, df_signed_tran_amt_1)
    df_balances_1 = balances_1(spark)
    df_WindowFunction_1 = WindowFunction_1(spark, df_balances_1)
    df_Filter_1 = Filter_1(spark, df_WindowFunction_1)
    df_WindowFunction_2 = WindowFunction_2(spark, df_Filter_1)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_WindowFunction_2)
    df_join_tran_balance = join_tran_balance(spark, df_sum_trans, df_SchemaTransform_1)
    df_unmatched = unmatched(spark, df_join_tran_balance)
    df_check_unmatches = check_unmatches(spark, df_unmatched)

    return df_check_unmatches
