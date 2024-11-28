import os
import sys
import requests
import json
import time
from itertools import count
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType





class Stage1:


    def __init__(self):
        self.list_url= "https://api.openbrewerydb.org/v1/breweries"
        self.urldb = "jdbc:postgresql://host.docker.internal:5499/postgres"
        self.properties = {
        "user": "postgres",
        "password": "secret",
        "driver": "org.postgresql.Driver"
        }
        self.schema = StructType([
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
        self.appName = "breweries_raw"
        self.JDBCfile="postgresql-42.7.4.jar"


    def create_spark(self, appName, JDBCpath):
        spark = SparkSession.builder\
            .appName(appName)\
            .config("spark.jars", JDBCpath)\
            .getOrCreate()
        return spark


    def query_API(list_url, params):
        return requests.get(list_url, params=params)


    def API_DF(self, spark, list_url, schema, pageStart=1, perPage=50):
        emptyRDD = spark.sparkContext.emptyRDD()
        lentotal=0
        df = spark.createDataFrame(data=emptyRDD, schema = schema)

        #len_lista_prev = 0
        len_lista_atual = 0
        for page in count(start=pageStart):#range(1,3):

            params={'page':page,
                    'per_page':perPage}
            try:
                response = self.query_API(list_url, params)
            except Exception as e:
                print(f"Erro na comunicação com a API: {e}")
            lista = json.loads(response.text.replace("null", '""'))
            #len(lista)
            #len_lista_prev = len_lista_atual
            len_lista_atual = len(lista)
            if len_lista_atual == 0:
                break
            try:
                dfcerv = spark.createDataFrame(data=lista, schema = schema)
            except Exception as e:
                print(f"Erro na criação de DataFrame: Página {page} com {perPage} entradas: {e}")
            print(f"pagina:{page}")
            lentotal+=len(lista)

            df=df.union(dfcerv)
            time.sleep(1)
        
        print(f"Total de entradas:{lentotal}")
        return df


    def write_to_db(self, df, urldb, tableName, properties):

        try:
            df.write.jdbc(urldb, tableName, mode="overwrite", properties=properties)
        except:
            print("Erro na gravaçao para banco Postgres")


    def stage1(self):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        __location__ = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
    
        JDBCpath = os.path.join(__location__, self.JDBCfile)
        
        spark = self.create_spark(self.appName, JDBCpath)
        df = self.API_DF(spark, self.list_url, self.schema, pageStart=1, perPage=50)
        df.printSchema()
        self.write_to_db(df, self.urldb, self.appName, self.properties)

        spark.stop()
        return 0
    