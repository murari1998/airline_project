# Databricks notebook source
dbutils.fs.ls("s3://us-airline-data/data/")

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("s3://us-airline-data/data/*/*")

display(df)   

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("Quarter", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("FlightDate", DateType(), True),
    StructField("Reporting_Airline", StringType(), True),
    StructField("DOT_ID_Reporting_Airline", IntegerType(), True),
    StructField("IATA_CODE_Reporting_Airline", StringType(), True),
    StructField("Tail_Number", StringType(), True),

    StructField("Flight_Number_Reporting_Airline", StringType(), True),
    StructField("OriginAirportID", IntegerType(), True),
    StructField("OriginAirportSeqID", IntegerType(), True),
    StructField("OriginCityMarketID", IntegerType(), True),
    StructField("Origin", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("OriginState", StringType(), True),
    StructField("OriginStateFips", StringType(), True),
    StructField("OriginStateName", StringType(), True),
    StructField("OriginWac", IntegerType(), True),
    StructField("DestAirportID", IntegerType(), True),

    StructField("DestAirportSeqID", IntegerType(), True),
    StructField("DestCityMarketID", IntegerType(), True),
    StructField("Dest", StringType(), True),
    StructField("DestCityName", StringType(), True),
    StructField("DestState", StringType(), True),
    StructField("DestStateFips", StringType(), True),
    StructField("DestStateName", StringType(), True),
    StructField("DestWac", IntegerType(), True),
    StructField("CRSDepTime", IntegerType(), True),
    StructField("DepTime", IntegerType(), True),
    StructField("DepDelay", FloatType(), True),
    StructField("DepDelayMinutes", FloatType(), True),
    StructField("DepDel15", IntegerType(), True),
    StructField("DepartureDelayGroups", IntegerType(), True),

    StructField("DepTimeBlk", StringType(), True),
    StructField("TaxiOut", FloatType(), True),
    StructField("WheelsOff", IntegerType(), True),
    StructField("WheelsOn", IntegerType(), True),
    StructField("TaxiIn", FloatType(), True),
    StructField("CRSArrTime", IntegerType(), True),
    StructField("ArrTime", IntegerType(), True),
    StructField("ArrDelay", FloatType(), True),
    StructField("ArrDelayMinutes", FloatType(), True),
    StructField("ArrDel15", IntegerType(), True),
    StructField("ArrivalDelayGroups", IntegerType(), True),
    StructField("ArrTimeBlk", StringType(), True),
    StructField("Cancelled", IntegerType(), True),
    StructField("CancellationCode", StringType(), True),
    StructField("Diverted", IntegerType(), True),
    StructField("CRSElapsedTime", FloatType(), True),
    StructField("ActualElapsedTime", FloatType(), True),
    StructField("AirTime", FloatType(), True),
    StructField("Flights", IntegerType(), True),
    StructField("Distance", FloatType(), True),
    StructField("DistanceGroup", IntegerType(), True),
    StructField("CarrierDelay", FloatType(), True),
    StructField("WeatherDelay", FloatType(), True),
    StructField("NASDelay", FloatType(), True),
    StructField("SecurityDelay", FloatType(), True),

    StructField("LateAircraftDelay", FloatType(), True),
    StructField("FirstDepTime", IntegerType(), True),
    StructField("TotalAddGTime", FloatType(), True),
    StructField("LongestAddGTime", FloatType(), True),
    StructField("DivAirportLandings", IntegerType(), True),
    StructField("DivReachedDest", IntegerType(), True),
    StructField("DivActualElapsedTime", FloatType(), True),
    StructField("DivArrDelay", FloatType(), True),
    StructField("DivDistance", FloatType(), True),
    # Diversion airports (Div1 to Div5)
    StructField("Div1Airport", StringType(), True),
    StructField("Div1AirportID", IntegerType(), True),
    StructField("Div1AirportSeqID", IntegerType(), True),
    StructField("Div1WheelsOn", IntegerType(), True),
    StructField("Div1TotalGTime", FloatType(), True),
    StructField("Div1LongestGTime", FloatType(), True),
    StructField("Div1WheelsOff", IntegerType(), True),
    StructField("Div1TailNum", StringType(), True),
    StructField("Div2Airport", StringType(), True),
    StructField("Div2AirportID", IntegerType(), True),
    StructField("Div2AirportSeqID", IntegerType(), True),
    StructField("Div2WheelsOn", IntegerType(), True),
    StructField("Div2TotalGTime", FloatType(), True),
    StructField("Div2LongestGTime", FloatType(), True),
    StructField("Div2WheelsOff", IntegerType(), True),
    StructField("Div2TailNum", StringType(), True),
    StructField("Div3Airport", StringType(), True),

    StructField("Div3AirportID", IntegerType(), True),
    StructField("Div3AirportSeqID", IntegerType(), True),
    StructField("Div3WheelsOn", IntegerType(), True),
    StructField("Div3TotalGTime", FloatType(), True),
    StructField("Div3LongestGTime", FloatType(), True),
    StructField("Div3WheelsOff", IntegerType(), True),
    StructField("Div3TailNum", StringType(), True),
    StructField("Div4Airport", StringType(), True),
    StructField("Div4AirportID", IntegerType(), True),
    StructField("Div4AirportSeqID", IntegerType(), True),
    StructField("Div4WheelsOn", IntegerType(), True),
    StructField("Div4TotalGTime", FloatType(), True),
    StructField("Div4LongestGTime", FloatType(), True),
    StructField("Div4WheelsOff", IntegerType(), True),
    StructField("Div4TailNum", StringType(), True),
    StructField("Div5Airport", StringType(), True),
    StructField("Div5AirportID", IntegerType(), True),
    StructField("Div5AirportSeqID", IntegerType(), True),
    StructField("Div5WheelsOn", IntegerType(), True),
    StructField("Div5TotalGTime", FloatType(), True),
    StructField("Div5LongestGTime", FloatType(), True),
    StructField("Div5WheelsOff", IntegerType(), True),
    StructField("Div5TailNum", StringType(), True),
    StructField("_c109", StringType(), True)
])

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", True)\
    .schema(schema)\
    .load("s3://us-airline-data/data/*/*")
display(df)

# COMMAND ----------

df.select("year", "month").distinct().orderBy("year", "month").display()

# COMMAND ----------

# 2022 dataframe
df_2022= spark.read \
    .option("header", True) \
    .schema(schema) \
    .csv("s3://us-airline-data/data/2022/*/*")
display(df_2022)


# COMMAND ----------

df_2022.count()

# COMMAND ----------

# 2021 dataframe
df_2021= spark.read \
    .option("header", True) \
    .schema(schema) \
    .csv("s3://us-airline-data/data/2021/*/*")
display(df_2021)

# COMMAND ----------

df_2021.count()

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(
    col("Flight_Number_Reporting_Airline").isNull()
).count()

# COMMAND ----------

df = spark.read.format("csv") \
.option("header","true") \
.option("nullValue","") \
.option("columnNameOfCorruptRecord","_corrupt_record") \
.schema(schema) \
.load("s3://us-airline-data/data/*/*/*")

display(df)

# COMMAND ----------

from pyspark.sql.functions import when, col, concat_ws
df = df.withColumn(
    "_corrupt_record_new",
    when(
        col("Flight_Number_Reporting_Airline").isNull() |
        col("FlightDate").isNull() |
        col("Distance").isNull() |
        col("ArrDelay").isNull(),
        concat_ws(",", *df.columns)
    ).otherwise(None)
)

display(df)

# COMMAND ----------

corrupt_df = df.filter(col("_corrupt_record_new").isNotNull())

display(corrupt_df)

# COMMAND ----------

corrupt_count = df.filter(col("_corrupt_record_new").isNotNull()).count()

print( corrupt_count)

# COMMAND ----------

df_permissive = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "") \
    .option("emptyValue","") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .load("s3://us-airline-data/data/*/*/*")
df_permissive.count()


# COMMAND ----------

df_dropmale = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "") \
    .option("emptyValue", "") \
    .option("mode", "DROPMALFORMED") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .load("s3://us-airline-data/data/*/*/*")

df_dropmale.count()

# COMMAND ----------

df_failfast = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue","") \
    .option("emptyValue","") \
    .option("mode", "FailFast") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .load("s3://us-airline-data/data/*/*/*")

display(df_failfast)

# COMMAND ----------

final_df =spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "") \
    .option("emptyValue","") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("s3://us-airline-data/data/*/*/*")

display(final_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

df_bronze = final_df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path")) \
    .withColumn(
        "_corrupt_record",
        col("_corrupt_record") if "_corrupt_record" in final_df.columns else lit(None)
    )

df_bronze.display()

# COMMAND ----------

df_bronze.count()

# COMMAND ----------

from pyspark.sql.functions import col, count, when

# Total records
total_count = df_bronze.count()

# Null count in flight_date
null_count = df_bronze.select(
    count(when(col("flightDate").isNull(), True))
).collect()[0][0]

# Percentage (type casting to float)
null_percentage = (float(null_count) / float(total_count)) * 100

print("Null Count:", null_count)
print("Null Percentage:", null_percentage)

# COMMAND ----------

from pyspark.sql.functions import col, when, avg

df_bronze.select(
    (avg(when(col("Distance").isNull(), 1).otherwise(0)) * 100).alias("distance_null_per"),
    (avg(when(col("ArrDelay").isNull(), 1).otherwise(0)) * 100).alias("arrival_delay_null_per"),
    (avg(when(col("DepDelay").isNull(), 1).otherwise(0)) * 100).alias("departure_delay_null_per"),
    (avg(when(col("Reporting_Airline").isNull(), 1).otherwise(0)) * 100).alias("reporting_airline_null_per")
).display()

# COMMAND ----------

from pyspark.sql.functions import min, max , month , year

df_bronze.select(
min("FlightDate").alias("min_date"),
max("FlightDate").alias("max_date"),
min(year("FlightDate")).alias("min year"),
max(year("FlightDate")).alias("max year"),
min(month("FlightDate")). alias("min_month"),
max(month("FlightDate")).alias("max_month")
).show()