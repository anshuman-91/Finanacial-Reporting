from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Generator(spark: SparkSession) -> (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame):
    from pyspark.shell import sc
    import datetime
    # acc_status
    acc_status_first_df = spark.createDataFrame(
        data = [(1, "A", "P1", datetime.date(2022, 5, 4), 0.0), (2, "B", "P1", datetime.date(2022, 5, 4), 0.0),
         (3, "A", "P2", datetime.date(2022, 5, 4), 0.0),],
        schema = 'acc_id int,person_id string,product_id string,business_date date,balance double'
    )
    acc_status_first_df = acc_status_first_df.coalesce(1)
    acc_status_second_df = spark.createDataFrame(
        data = [(1, "A", "P1", datetime.date(2022, 5, 5), 100.0), (2, "B", "P1", datetime.date(2022, 5, 5), 150.0),
         (3, "A", "P2", datetime.date(2022, 5, 5), 1000.0),],
        schema = 'acc_id int,person_id string,product_id string,business_date date,balance double'
    )
    acc_status_second_df = acc_status_second_df.coalesce(1)
    # transactions
    transactions_df = spark.createDataFrame(
        data = [(1, "a1x", datetime.date(2022, 5, 5), 200.0, "CREDIT",
          datetime.datetime.strptime("2022-05-05T01:30:00.184-05:00", "%Y-%m-%dT%H:%M:%S.%f%z")),
         (1, "a1y", datetime.date(2022, 5, 5), 110.0, "DEBIT",
          datetime.datetime.strptime("2022-05-05T09:45:00.193-05:00", "%Y-%m-%dT%H:%M:%S.%f%z")),
         (1, "a1z", datetime.date(2022, 5, 5), 10.0, "INTEREST",
          datetime.datetime.strptime("2022-05-05T12:00:00.564-05:00", "%Y-%m-%dT%H:%M:%S.%f%z")),
         (2, "a2a", datetime.date(2022, 5, 5), 50.0, "CREDIT",
          datetime.datetime.strptime("2022-05-05T11:31:22.231-05:00", "%Y-%m-%dT%H:%M:%S.%f%z")),
         (2, "a2b", datetime.date(2022, 5, 5), 200.0, "CREDIT",
          datetime.datetime.strptime("2022-05-05T05:04:21.871-05:00", "%Y-%m-%dT%H:%M:%S.%f%z")),
         (2, "a2c", datetime.date(2022, 5, 5), 100.0, "DEBIT",
          datetime.datetime.strptime("2022-05-05T12:05:11.135-05:00", "%Y-%m-%dT%H:%M:%S.%f%z")),
         (3, "a2d", datetime.date(2022, 5, 5), 449.5, "CREDIT",
          datetime.datetime.strptime("2022-05-05T15:05:15.154-05:00", "%Y-%m-%dT%H:%M:%S.%f%z")),
         (3, "a2e", datetime.date(2022, 5, 5), 449.5, "CREDIT",
          datetime.datetime.strptime("2022-05-05T18:05:31.130-05:00", "%Y-%m-%dT%H:%M:%S.%f%z")),
         (3, "a2f", datetime.date(2022, 5, 5), 100.0, "INTEREST",
          datetime.datetime.strptime("2022-05-05T22:45:55.116-05:00", "%Y-%m-%dT%H:%M:%S.%f%z")),],
        schema = 'acc_id int,tran_id string,business_date date,tran_amount double,tran_type string,tran_ts timestamp'
    )
    transactions_df = transactions_df.coalesce(1)
    # people
    person_json_1 = '''{"id":"A","name":"James Jones","email":"james_earl_93@gmail.com","updated_at":"2022-05-05T12:00:00.000-05:00","addresses":[{"postal_code":"A24011","address_line1":"4638 White Pine Lane","address_line2":"Roanoke, Virginia","type":"PRIMARY"},{"postal_code":"CD36582","address_line1":"1268 George Avenue","address_line2":"Theodore, Alabama","type":"ALTERNATE"},{"postal_code":"AX85003","address_line1":"3881 Coplin Avenue","address_line2":"Phoenix, Arizona","type":"ALTERNATE"}]}'''
    person_json_2 = '''{"id":"B","name":"Jonas Smith","updated_at":"2022-05-05T16:12:44.252-05:00","email":"jjsmith89@yahoo.com","addresses":[{"postal_code":"AB33614","address_line1":"22897 Maryland Avenue, Tampa, Florida","type":"PRIMARY"},{"postal_code":"CD21321","address_line1":"559 Lake Floyd Circle, New Castle, Delaware","type":"ALTERNATE"}]}'''
    person_json_3 = '''{"id":"C","name":"Adam Boorman","email":"adamb213@gmail.com","updated_at":"2022-05-05T05:32:55.864-05:00","addresses":[{"postal_code":"XR96814","address_line1":"4558 Indiana Circle","address_line2":"Honolulu, Hawai","type":"PRIMARY"},{"postal_code":"CC47906","address_line1":"26 Glenwood Drive","address_line2":"West Lafayette, IN","type":"ALTERNATE"}]}'''
    people_df = spark.read.json(sc.parallelize([person_json_1, person_json_2, person_json_3]))
    people_df = people_df.coalesce(1)
    # products
    product_json_1 = '''{"id":"P1","name":"Fixed Term","properties":{"bonus_rate":1.512,"lock_in_period":1},"slug":"Fixed Duration 1 Year","updated_at":"2022-05-05T12:00:00.000-05:00"}'''
    product_json_2 = '''{"id":"P2","name":"Savings","properties":{"bonus_rate":1.512},"slug":"Online Savings Account","updated_at":"2022-05-05T12:00:00.000-05:00"}'''
    products_df = spark.read.json(sc.parallelize([product_json_1, product_json_2]))
    products_df = products_df

    return (acc_status_first_df, acc_status_second_df, transactions_df, people_df, products_df)
