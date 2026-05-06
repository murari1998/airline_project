# Databricks notebook source
dbutils.fs.ls("s3://us-airline-data/data/")

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .load("s3://us-airline-data/data/*/*/*")

# COMMAND ----------


from pyspark.sql.types import *

flight_schema = StructType([

    StructField("Year", IntegerType(), True),
    StructField("Quarter", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),

    StructField("FlightDate", StringType(), True),  # later cast to date

    StructField("Reporting_Airline", StringType(), True),
    StructField("DOT_ID_Reporting_Airline", IntegerType(), True),
    StructField("IATA_CODE_Reporting_Airline", StringType(), True),
    StructField("Tail_Number", StringType(), True),

    StructField("Flight_Number_Reporting_Airline", IntegerType(), True),  

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
    StructField("DepDelay", DoubleType(), True),
    StructField("DepDelayMinutes", DoubleType(), True),
    StructField("DepDel15", DoubleType(), True),
    StructField("DepartureDelayGroups", IntegerType(), True),
    StructField("DepTimeBlk", StringType(), True),

    StructField("TaxiOut", DoubleType(), True),
    StructField("WheelsOff", IntegerType(), True),
    StructField("WheelsOn", IntegerType(), True),
    StructField("TaxiIn", DoubleType(), True),

    StructField("CRSArrTime", IntegerType(), True),
    StructField("ArrTime", IntegerType(), True),
    StructField("ArrDelay", DoubleType(), True),
    StructField("ArrDelayMinutes", DoubleType(), True),
    StructField("ArrDel15", DoubleType(), True),
    StructField("ArrivalDelayGroups", IntegerType(), True),
    StructField("ArrTimeBlk", StringType(), True),

    StructField("Cancelled", DoubleType(), True),
    StructField("CancellationCode", StringType(), True),
    StructField("Diverted", DoubleType(), True),

    StructField("CRSElapsedTime", DoubleType(), True),
    StructField("ActualElapsedTime", DoubleType(), True),
    StructField("AirTime", DoubleType(), True),
    StructField("Flights", DoubleType(), True),

    StructField("Distance", DoubleType(), True),
    StructField("DistanceGroup", IntegerType(), True),

    StructField("CarrierDelay", DoubleType(), True),
    StructField("WeatherDelay", DoubleType(), True),
    StructField("NASDelay", DoubleType(), True),
    StructField("SecurityDelay", DoubleType(), True),
    StructField("LateAircraftDelay", DoubleType(), True),

    StructField("FirstDepTime", IntegerType(), True),
    StructField("TotalAddGTime", DoubleType(), True),
    StructField("LongestAddGTime", DoubleType(), True),

    StructField("DivAirportLandings", DoubleType(), True),
    StructField("DivReachedDest", DoubleType(), True),
    StructField("DivActualElapsedTime", DoubleType(), True),
    StructField("DivArrDelay", DoubleType(), True),
    StructField("DivDistance", DoubleType(), True),

    StructField("Div1Airport", StringType(), True),
    StructField("Div1AirportID", IntegerType(), True),
    StructField("Div1AirportSeqID", IntegerType(), True),
    StructField("Div1WheelsOn", IntegerType(), True),
    StructField("Div1TotalGTime", DoubleType(), True),
    StructField("Div1LongestGTime", DoubleType(), True),
    StructField("Div1WheelsOff", IntegerType(), True),
    StructField("Div1TailNum", StringType(), True),

    StructField("Div2Airport", StringType(), True),
    StructField("Div2AirportID", IntegerType(), True),
    StructField("Div2AirportSeqID", IntegerType(), True),
    StructField("Div2WheelsOn", IntegerType(), True),
    StructField("Div2TotalGTime", DoubleType(), True),
    StructField("Div2LongestGTime", DoubleType(), True),
    StructField("Div2WheelsOff", IntegerType(), True),
    StructField("Div2TailNum", StringType(), True),

    StructField("Div3Airport", StringType(), True),
    StructField("Div3AirportID", IntegerType(), True),
    StructField("Div3AirportSeqID", IntegerType(), True),
    StructField("Div3WheelsOn", IntegerType(), True),
    StructField("Div3TotalGTime", DoubleType(), True),
    StructField("Div3LongestGTime", DoubleType(), True),
    StructField("Div3WheelsOff", IntegerType(), True),
    StructField("Div3TailNum", StringType(), True),

    StructField("Div4Airport", StringType(), True),
    StructField("Div4AirportID", IntegerType(), True),
    StructField("Div4AirportSeqID", IntegerType(), True),
    StructField("Div4WheelsOn", IntegerType(), True),
    StructField("Div4TotalGTime", DoubleType(), True),
    StructField("Div4LongestGTime", DoubleType(), True),
    StructField("Div4WheelsOff", IntegerType(), True),
    StructField("Div4TailNum", StringType(), True),

    StructField("Div5Airport", StringType(), True),
    StructField("Div5AirportID", IntegerType(), True),
    StructField("Div5AirportSeqID", IntegerType(), True),
    StructField("Div5WheelsOn", IntegerType(), True),
    StructField("Div5TotalGTime", DoubleType(), True),
    StructField("Div5LongestGTime", DoubleType(), True),
    StructField("Div5WheelsOff", IntegerType(), True),
    StructField("Div5TailNum", StringType(), True),

    StructField("_c109", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------


df_full = spark.read.format("csv") \
    .option("header", "true") \
    .schema(flight_schema) \
    .load("s3://us-airline-data/data/*/*/*")

# COMMAND ----------

# required columns
req_col = [
    "FlightDate",
    "Reporting_Airline",
    "IATA_CODE_Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "OriginCityName",
    "OriginState",
    "Dest",
    "DestCityName",
    "DestState",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",
    "DepartureDelayGroups",
    "CRSArrTime",
    "ArrTime",
    "ArrDelayMinutes",
    "ArrivalDelayGroups",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
    "Flights",
    "Distance",
    "DistanceGroup",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
    "_corrupt_record"
]

# COMMAND ----------

df = df_full.select(req_col)
display(df)

# COMMAND ----------

df_full = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .option("nullValue", "") \
    .option("emptyValue", "") \
    .schema(flight_schema) \
    .load("s3://us-airline-data/data/*/*/*")

# COMMAND ----------

df = df_full.select(req_col)
display(df)

# COMMAND ----------

colum = [
    "Distance",
    "DepDelay",
    "Reporting_Airline",
    "Origin",
    "Cancelled",
    "Dest"
]

null_dict = {c: 0 for c in colum}

# COMMAND ----------

total_count = df.count()

# COMMAND ----------

from pyspark.sql.functions import col

for c in null_dict.keys():
    null_count = df.filter(col(c).isNull()).count()
    null_dict[c] = (null_count / total_count) * 100

null_dict

# COMMAND ----------

colum = ["FlightDate", "Reporting_Airline", "Origin", "Dest"]

null_flag_dict = {c: f"{c}_null" for c in colum}

# COMMAND ----------

from pyspark.sql.functions import when, col

for c, new_col in null_flag_dict.items():
    df = df.withColumn(
        new_col,
        when(col(c).isNull(), 1).otherwise(0)
    )

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select(
    "FlightDate", "FlightDate_null",
    "Reporting_Airline", "Reporting_Airline_null",
    "Origin", "Origin_null",
    "Dest", "Dest_null"
).show(10)

# COMMAND ----------

from pyspark.sql.functions import when, col

df = df.withColumn(
    "dep_delay_flag",
    when((col("DepDelay") >= -120) & (col("DepDelay") <= 1440), 0).otherwise(1)
)

# COMMAND ----------

df = df.withColumn(
    "same_origin_destination",
    when(col("Origin") == col("Dest"), 1).otherwise(0)
)

# COMMAND ----------

df = df.withColumn(
    "is_domestic_flag",
    when((col("Distance") >= 1) & (col("Distance") <= 10000), 0).otherwise(1)
)

# COMMAND ----------

display(df)

# COMMAND ----------


df_bronze = df

# COMMAND ----------

# creating  a bronze_path

bronze_path = "s3://us-airline-data/bronze/flight"

# COMMAND ----------

from pyspark.sql.functions import year, month, col

df_bronze = df_bronze.withColumn("year", year(col("FlightDate"))) \
                     .withColumn("month", month(col("FlightDate")))

# COMMAND ----------

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .save(bronze_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists flight_analystics;

# COMMAND ----------

# MAGIC %sql
# MAGIC use flight_analystics;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists bronze_flights
# MAGIC using delta location 's3://us-airline-data/bronze/';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flight_analystics.bronze_flights;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM flight_analystics.bronze_flights;

# COMMAND ----------



# COMMAND ----------

df = spark.read \
    .format("delta") \
    .load("s3://us-airline-data/bronze/")

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.columns

# COMMAND ----------

select_colum = ["FlightDate_null","Reporting_Airline_null","Origin_null","Dest_null"]
print(select_colum)

# COMMAND ----------

from pyspark.sql.functions import col, when

sel_colm = ["FlightDate_null","Origin_null"]

df_nulls = df

for c in sel_colm:
     
    df_nulls = df_nulls.withColumn(
        f"null_{c}",
        when(col(c).isNull(), 1).otherwise(0)
    )

display(df_nulls)

# COMMAND ----------

df_final = df_nulls.withColumn(
    "result_sum",
    col("FlightDate_null") + col("Origin_null")
)

df_final.select(
        "FlightDate_null",
        "Origin_null",
        "result_sum").show()
