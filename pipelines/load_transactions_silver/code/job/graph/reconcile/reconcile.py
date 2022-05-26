from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from . import *

def reconcile(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_signed_tran_amt_1 = signed_tran_amt_1(spark, in0)
    df_sum_trans = sum_trans(spark, df_signed_tran_amt_1)
    df_balances = balances(spark)
    df_add_row_num2 = add_row_num2(spark, df_balances)
    df_row_num_eq1 = row_num_eq1(spark, df_add_row_num2)
    df_prev_day_balance = prev_day_balance(spark, df_row_num_eq1)
    df_balance_change = balance_change(spark, df_prev_day_balance)
    df_join_tran_balance = join_tran_balance(spark, df_sum_trans, df_balance_change)
    df_unmatched = unmatched(spark, df_join_tran_balance)
    df_check_unmatches = check_unmatches(spark, df_unmatched)

    return df_check_unmatches
