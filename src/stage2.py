from pyspark.sql import SparkSession
import os


class Stage2:

    def __init__(self):
        self.appName = "breweries_part"
        self.tableName = "breweries_raw"
        self.urlDb = "jdbc:postgresql://host.docker.internal:5499/postgres"
        self.partquetFile = "breweries_part_parquet"
        self.JDBCfile="postgresql-42.7.4.jar"
        self.partitionBy = "country"


    def create_spark(self, appName, JDBCpath):
        spark = SparkSession.builder\
            .appName(appName)\
            .config("spark.jars", JDBCpath)\
            .getOrCreate()
        return spark


    def read_raw(self, spark, urlDb, tableName):
        df = spark.read \
            .format("jdbc") \
            .option("url", urlDb) \
            .option("dbtable", tableName) \
            .option("user", "postgres") \
            .option("password", "secret") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df.printSchema()

        df.createOrReplaceTempView("breweries_part")

        #Efetua limpeza de entradas duplicadas (id diferente, todos outros campos idênticos)
        filterSQL = spark.sql("""select max(id) as id, name, brewery_type, address_1, address_2, address_3, city, state_province, postal_code, country, longitude, latitude, phone, website_url, state, street
        from breweries_part 
        group by name, brewery_type, address_1, address_2, address_3, city, state_province, postal_code, country, longitude, latitude, phone, website_url, state, street""")

        return filterSQL


    def write_parquet_partitioned(self, df, parquetPath, partitionBy):
        df.write.mode("overwrite") \
            .partitionBy(partitionBy) \
            .parquet(parquetPath)


    def stage2(self):
        
        __location__ = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
    
        JDBCpath = os.path.join(__location__, self.JDBCfile)
        parquetPath = os.path.join(__location__, self.partquetFile)

        spark = self.create_spark(self.appName, JDBCpath)
        df = self.read_raw(spark, self.urlDb, self.tableName)
        self.write_parquet_partitioned(df, parquetPath, self.partitionBy)
        spark.stop()
        return 0
