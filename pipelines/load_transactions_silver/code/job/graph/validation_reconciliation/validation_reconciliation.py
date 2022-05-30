from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from . import *

def validation_reconciliation(spark: SparkSession, in0: DataFrame) -> None:
    df_validate_transacions = validate_transacions(spark, in0)
    df_signed_tran_amt_1 = signed_tran_amt_1(spark, df_validate_transacions)
    df_sum_trans = sum_trans(spark, df_signed_tran_amt_1)
    df_acc_status_silver = acc_status_silver(spark)
    df_add_row_num = add_row_num(spark, df_acc_status_silver)
    df_row_num_eq1 = row_num_eq1(spark, df_add_row_num)
    df_prev_day_balance = prev_day_balance(spark, df_row_num_eq1)
    df_balance_change = balance_change(spark, df_prev_day_balance)
    df_join_tran_balance = join_tran_balance(spark, df_sum_trans, df_balance_change)
    df_unmatched = unmatched(spark, df_join_tran_balance)
    check_unmatches(spark, df_unmatched)
