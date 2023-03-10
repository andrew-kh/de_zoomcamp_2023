import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.functions import max

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read.option("header", "true").csv('/mnt/c/Users//vs-projects/de_zoomcamp/Week5/data/fhv_tripdata_2021-06.csv')

df.show()

df_for_schema = pd.read_csv('/mnt/c/Users//vs-projects/de_zoomcamp/Week5/data/head.csv')

df_for_schema.Affiliated_base_number = df_for_schema.Affiliated_base_number.fillna('NA')

spark.createDataFrame(df_for_schema).schema

schema = types.StructType([
                types.StructField('dispatching_base_num', types.StringType(), True),
                types.StructField('pickup_datetime', types.StringType(), True),
                types.StructField('dropoff_datetime', types.StringType(), True),
                types.StructField('PULocationID', types.LongType(), True),
                types.StructField('DOLocationID', types.LongType(), True),
                types.StructField('SR_Flag', types.StringType(), True),
                types.StructField('Affiliated_base_number', types.StringType(), True)
        ])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('/mnt/c/Users//vs-projects/de_zoomcamp/Week5/data/fhv_tripdata_2021-06.csv')

df = df.repartition(12)

df.write.parquet('/mnt/c/Users//vs-projects/de_zoomcamp/Week5/data/partitions')

df.filter(col("pickup_datetime").like("%06-15%")).show()

df.filter(col("pickup_datetime").like("%06-15%")).count()

df = df.withColumn("pickup_datetime_timestamp",to_timestamp("pickup_datetime"))
df = df.withColumn("dropoff_datetime_timestamp",to_timestamp("dropoff_datetime"))
df = df.withColumn('diff_seconds',col("dropoff_datetime_timestamp").cast("long") - col('pickup_datetime_timestamp').cast("long"))
df = df.withColumn('diff_hours',col('diff_seconds')/3600)

df.select(max(df.diff_hours).alias("max_trip")).show()


df_zones = spark.read.option("header", "true").csv('/mnt/c/Users//vs-projects/de_zoomcamp/Week5/data/taxi_zone_lookup.csv')

df_zones = df_zones.withColumn("LocationID",df_zones.LocationID.cast(types.IntegerType()))

df = df.join(df_zones,df.PULocationID ==  df_zones.LocationID,"left").show(truncate=False)

df.dropDuplicates(["PULocationID"]).select("PULocationID").show()

df.groupBy("PULocationID").count().orderBy(col("count").desc()).show()
