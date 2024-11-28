import os 
import sys
import json

from unittest import mock
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, LongType
from pyspark import pyspark.testing.assertSchemaEqual

from stage3 import breweries_grouped

schemaIn = StructType([
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

schemaOut=StructType([
        StructField('country', StringType(), True),
        StructField('brewery_type', StringType(), True),
        StructField('breweries', LongType(), False)
    ])


def create_spark():
        spark = SparkSession.builder\
            .getOrCreate()
        return spark

def test_breweries_grouped():
    emptyRDD = spark.sparkContext.emptyRDD()

    df = spark.createDataFrame(data=emptyRDD, schema = schemaIn)
    df_group = spark.createDataFrame(data=emptyRDD, schema = schemaOut)

    df_test=breweries_grouped(df)
    
    assertSchemaEqual(df_group, df_test)

