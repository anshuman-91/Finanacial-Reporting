from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def check_unmatches(spark: SparkSession, in0: DataFrame):

    if (in0.count() > 0):
        raise Exception("Transactions don't add up to balance")

    return 
