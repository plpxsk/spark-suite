from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


input_schema = StructType([
    StructField('StoreID', IntegerType(), True),
    StructField('Location', StringType(), True),
    StructField('Date', StringType(), True),
    StructField('ItemCount', IntegerType(), True)
])

input_data = [(1, "Bangalore", "2021-12-01", 5),
              (2, "Bangalore", "2021-12-01", 3),
              (5, "Amsterdam", "2021-12-02", 10),
              (6, "Amsterdam", "2021-12-01", 1),
              (8, "Warsaw", "2021-12-02", 15),
              (7, "Warsaw", "2021-12-01", 99)]

spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("PySpark-example")
         .config('spark.port.maxRetries', 30)
         .getOrCreate()
         )

input_df = spark.createDataFrame(data=input_data, schema=input_schema)

print(input_df.count())
