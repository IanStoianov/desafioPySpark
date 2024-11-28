from pyspark.sql import SparkSession
import os

class Stage3:
    
    def __init__(self):
        self.appName = "breweries_grouped"
        self.partquetFilePart = "breweries_part_parquet"
        self.parquetFileGroup = "breweries_group.parquet"
        self.partitionBy = "country"


    def create_spark(self, appName):
        spark = SparkSession.builder\
            .appName(appName)\
            .getOrCreate()
        return spark
    
    
    def read_parquet(self, spark, parquetPath):
        return spark.read.parquet(parquetPath)


    def breweries_grouped(self, spark, df):
        
        df.createOrReplaceTempView("breweries_part")
    
        groupSQL = spark.sql("""select country, brewery_type, count(*) as breweries from breweries_part
        group by country, brewery_type""")
        groupSQL.printSchema()

        return groupSQL
    
    
    def write_parquet_partitioned(self, df, parquetPath, partitionBy):
        df.write.mode("overwrite") \
            .partitionBy(partitionBy) \
            .parquet(parquetPath)
    
    
    def stage3(self):
        __location__ = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))

        parquetPathPart = os.path.join(__location__, self.partquetFilePart)
        parquetPathGroup = os.path.join(__location__, self.parquetFileGroup)
    
        spark = self.create_spark(self.appName)
        df = self.read_parquet(spark, parquetPathPart)
        groupedDF = self.breweries_grouped(spark, df)
        self.write_parquet_partitioned(groupedDF, parquetPathGroup, self.partitionBy)
        spark.stop()
        return 0
