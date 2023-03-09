import unittest
from datetime import datetime

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from etl.etl import transform_data


class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession
            .builder
            .master("local[*]")
            .appName("Unit-tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_etl(self):
        input_schema = StructType([
            StructField('StoreID', IntegerType(), True),
            StructField('Location', StringType(), True),
            StructField('Date', StringType(), True),
            StructField('ItemCount', IntegerType(), True)
        ])

        expected_schema = StructType([
            StructField('Location', StringType(), True),
            StructField('TotalItemCount', LongType(), True)
        ])

        input_data = [(1, "Bangalore", "2021-12-01", 5),
                      (2, "Bangalore", "2021-12-01", 3),
                      (5, "Amsterdam", "2021-12-02", 10),
                      (6, "Amsterdam", "2021-12-01", 1),
                      (8, "Warsaw", "2021-12-02", 15),
                      (7, "Warsaw", "2021-12-01", 99)]

        expected_data = [("Bangalore", 8),
                         ("Warsaw", 114),
                         ("Amsterdam", 11)]

        input_df = self.spark.createDataFrame(data=input_data,
                                              schema=input_schema)

        expected_df = self.spark.createDataFrame(data=expected_data,
                                                 schema=expected_schema)

        # we are testing this function
        transformed_df = transform_data(input_df)

        # assertions to test that the function works
        def field_list(fields): return (
            fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]

        res = set(fields1) == set(fields2)

        # unittest assertions here
        self.assertTrue(res)

        self.assertEqual(
            sorted(expected_df.collect()),
            sorted(transformed_df.collect())
        )
