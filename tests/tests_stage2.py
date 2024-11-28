import os 
import sys
import json

from unittest import mock
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType


from stage2 import filter_df_duplicate

schema = StructType([
      StructField('id', StringType(), False),
      StructField('name', StringType(), True),
      StructField('brewery_type', StringType(), True),
      StructField('address_1', StringType(), True),
      StructField('address_2', StringType(), True),
      StructField('address_3', StringType(), True),
      StructField('city', StringType(), True),
      StructField('state_province', StringType(), True),
      StructField('postal_code', StringType(), True),
      StructField('country', StringType(), True),
      StructField('longitude', StringType(), True),
      StructField('latitude', StringType(), True),
      StructField('phone', StringType(), True),
      StructField('website_url', StringType(), True),
      StructField('state', StringType(), True),
      StructField('street', StringType(), True)
      ])




from stage2 import filter_df_duplicate



def create_spark():
        spark = SparkSession.builder\
            .getOrCreate()
        return spark

def test_filter_df_duplicate():
    emptyRDD = spark.sparkContext.emptyRDD()

    df = spark.createDataFrame(data=emptyRDD, schema = schema)
    df_filter = filter_df_duplicate(df)

    assert len(df_filter.head(1)) == 0
