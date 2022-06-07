
class Config:

    def __init__(self, fabricName: str=None):
        self.spark = None
        self.update(fabricName)

    def updateSpark(self, spark):
        self.spark = spark

    def get_dbutils(self, spark):

        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            try:
                import IPython
                dbutils = IPython.get_ipython().user_ns["dbutils"]
            except ModuleNotFoundError:
                try:
                    from prophecy.test.utils import ProphecyDBUtil
                    dbutils = ProphecyDBUtil
                except ModuleNotFoundError:
                    dbutils = prophecydbutils

        return dbutils

    def update(self, fabricName: str):
        self.fabricName = fabricName
