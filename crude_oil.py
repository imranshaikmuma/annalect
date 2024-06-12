from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Window


class CrudeOil:
    def __init__(self, file_name):
        """ Creates spark session  and set logging level"""
        self.file_name = file_name
        self.process_name = self.__class__.__name__
        self.spark = SparkSession.builder.appName(self.process_name).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

        self.schema = StructType([
            StructField("year", IntegerType(), True), StructField("month", IntegerType(), True),
            StructField("originName", StringType(), True), StructField("originTypeName", StringType(), True),
            StructField("destinationName", StringType(), True), StructField("destinationTypeName", StringType(), True),
            StructField("gradeName", StringType(), True), StructField("quantity", StringType(), True)
        ])

    def read_csv(self):
        """
        Reads input csv file while inferring schema
        :return: Spark dataframe
        """
        df = self.spark.read.format("csv").option("delimiter", ",").option("header", "true"). \
            option("inferSchema", "true").load(self.file_name)
        return df

    def albania_stats(self):
        """
        Calculates: What are the top 5 destinations for oil produced in Albania?
        :return: destination df
        """
        print('What are the top 5 destinations for oil produced in Albania?')
        albania_df = self.read_csv().where(F.col('originName') == 'Albania').groupby(F.col('destinationName')). \
            agg(F.sum(F.col('quantity').cast('integer')).alias('total_quantity'))
        return albania_df.sort(F.col('total_quantity').desc()).select(F.col('destinationName')).show(5, False)

    def uk_stats(self):
        """
        calculates: For UK, which destinations have a total quantity greater than 100,000?
        :return: destinations df
        """
        print('For UK, which destinations have a total quantity greater than 100,000?')
        uk_df = self.read_csv().where(F.col('originName') == 'United Kingdom').groupby(F.col('destinationName')). \
            agg(F.sum(F.col('quantity').cast('integer')).alias('total_quantity'))
        return uk_df.where(F.col('total_quantity') > 100000).select(F.col('destinationName')).show()

    def most_exported_grade(self):
        """
        Calculates: What was the most exported grade for each year and origin?
        :return: most exported grade df
        """
        print('What was the most exported grade for each year and origin?')
        grade_per_year = self.read_csv().groupby(F.col('year'), F.col('originName'), F.col('gradeName')). \
            agg(F.sum(F.col('quantity').cast('integer')).alias('total_quantity'))
        w = Window.partitionBy("year", "originName").orderBy(F.col('total_quantity').desc())
        grade_per_year_output = grade_per_year.withColumn('rank', F.dense_rank().over(w))
        return grade_per_year_output.where(F.col('rank') == 1).select('year', 'originName', 'gradeName') \
            .show(grade_per_year_output.count(), False)

    def write_to_iceberg(self):
        """
        Sets spark conf to convert csv to iceberg format
        :return: DF that reads from iceberg format
        """
        self.spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        self.spark.conf.set("spark.sql.catalog.spark_catalog.type", "hadoop")
        self.spark.conf.set("spark.sql.catalog.spark_catalog.warehouse", "/app/spark-warehouse/")
        self.spark.conf.set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        self.spark.conf.set("spark.sql.catalog.demo.type", "hadoop")
        self.spark.conf.set("spark.sql.catalog.demo.warehouse", "/app/spark-warehouse/")

        self.spark.sql("""
                CREATE TABLE IF NOT EXISTS spark_catalog.iceberg.crude_oil (
                  year INT,
                  month INT,
                  originName string,
                  originTypeName string,
                  destinationName string,
                  destinationTypeName string,
                  gradeName string,
                  quantity INT
                )
                USING iceberg
                PARTITIONED BY (originTypeName)
                """)

        self.read_csv().write.format("iceberg").mode("overwrite").saveAsTable("spark_catalog.iceberg.crude_oil")
        iceberg_df = self.spark.sql("""
                select * from spark_catalog.iceberg.crude_oil
                """)
        return iceberg_df.show()


if __name__ == '__main__':
    crude_oil = CrudeOil('/app/data.csv')
    crude_oil.albania_stats()
    crude_oil.uk_stats()
    crude_oil.most_exported_grade()
    crude_oil.write_to_iceberg()
