# Databricks notebook source
df=spark.read.option("multiLine", "true").json("s3://us-airline-data/airport-data/airports_multiline.json")
# display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

schema = StructType([
    StructField("city", StringType(), True),
    StructField("code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("elevation", LongType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("tz", StringType(), True)
])

# COMMAND ----------

df = spark.read \
    .option("multiLine", "true") \
    .schema(schema) \
    .json("s3://us-airline-data/airport-data/airports_multiline.json")

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df_us = df.filter(col("country") == "US")
display(df_us)

# COMMAND ----------

df_us.count()

# COMMAND ----------

df_us.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://us-airline-data/airport-data/delta/")

# COMMAND ----------



# COMMAND ----------

# day 5

# COMMAND ----------

df = spark.read.format("delta").load("s3://us-airline-data/bronze/")
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy(
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest"
).orderBy("FlightDate") 

df_window = df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

# COMMAND ----------

from pyspark.sql.functions import col, floor

df = df.withColumn(
    "crsdeptime_total_minutes",
    (floor(col("CRSDepTime") / 100) * 60) + (col("CRSDepTime") % 100)
).withColumn(
    "crs_arrtime_total_minutes",
    (floor(col("CRSArrTime") / 100) * 60) + (col("CRSArrTime") % 100)
)

display(df)


# COMMAND ----------

# hours_to_minutes Function 
# Function to convert HHMM format to total minutes
# If value is 2400 then return 0
from pyspark.sql.functions import col, when, floor

def hours_to_minuet(column_name):
    return when(
        col(column_name).isNull(), None
    ).when(
        col(column_name).cast("int") == 2400, 0
    ).otherwise(
        (floor(col(column_name).cast("int") / 100) * 60) + (col(column_name).cast("int") % 100)
    )

df = df.withColumn(
    "crs_departure_time_minutes",
    hours_to_minuet("CRSDepTime")
).withColumn(
    "crs_arrival_time_minutes",
    hours_to_minuet("CRSArrTime")
)


# COMMAND ----------

display(df)

# COMMAND ----------

#  is_cancelled, is_diverted, is_departure_delay Boolean Columns
from pyspark.sql.functions import col, when

df = df.withColumn(
    "is_cancelled",
    when(col("Cancelled").cast("int") == 1, True).otherwise(False)
).withColumn(
    "is_divarted",
    when(col("Diverted").cast("int") == 1, True).otherwise(False)
).withColumn(
    "is_departure_delay",
    when(col("DepDelay").cast("double") > 0, True).otherwise(False)
).withColumn(
    "is_arrival_delayed",
    when(col("ArrDelayMinutes").cast("double") > 0, True).otherwise(False)
)

# COMMAND ----------

display(df)

# COMMAND ----------

# Create arrival delay category based on ArrDelayMinutes column
#  if less than 0 = Early
# 0 to 14  = On Time
# 15 to 44 = Minor Delay
# 45 to 120  = Major Delay
# above 120   = Severe Delay
# cancelled flight = Cancelled

df = df.withColumn(
    "arrival_delay_category_bucket",
    when(col("Cancelled") == 1, "Cancelled")
    .when(col("ArrDelayMinutes") < 0, "Early")
    .when((col("ArrDelayMinutes") >= 0) & (col("ArrDelayMinutes") <= 14), "On Time")
    .when((col("ArrDelayMinutes") >= 15) & (col("ArrDelayMinutes") <= 44), "Minor Delay")
    .when((col("ArrDelayMinutes") >= 45) & (col("ArrDelayMinutes") <= 120), "Major Delay")
    .when(col("ArrDelayMinutes") > 120, "Severe Delay")
    .otherwise(None)
)

display(df.select("ArrDelayMinutes", "Cancelled", "arrival_delay_category_bucket"))

# COMMAND ----------

#  Date Columns Extract karna
# Extract date parts from FlightDate column
from pyspark.sql.functions import year, month, quarter, dayofweek, to_date

df = df \
    .withColumn("FlightDate", to_date(col("FlightDate"))) \
    .withColumn("flight_year", year(col("FlightDate"))) \
    .withColumn("flight_month", month(col("FlightDate"))) \
    .withColumn("flight_quarter", quarter(col("FlightDate"))) \
    .withColumn("day_of_week", dayofweek(col("FlightDate")))

display(df.select("FlightDate", "flight_year", "flight_month", "flight_quarter", "day_of_week"))

# COMMAND ----------

# airline_code Column banana
# Create airline_code from IATA_CODE_Reporting_Airline,origen_code,destination_code in uppercase
from pyspark.sql.functions import col, upper

df = df.withColumn(
    "airline_code",
    upper(col("IATA_CODE_Reporting_Airline"))
).withColumn(
    "origin_code",
    upper(col("Origin"))
).withColumn(
    "destination_code",
    upper(col("Dest"))
)

# COMMAND ----------

# cancellation_reason Column banana
# Create cancellation_reason from CancellationCode
# A = Carrier, B = Weather, C = National Security, D = Security Concern, else = None

df = df.withColumn(
    "cancellation_reason",
    when(col("CancellationCode") == "A", "Carrier")
    .when(col("CancellationCode") == "B", "Weather")
    .when(col("CancellationCode") == "C", "National_Security")
    .when(col("CancellationCode") == "D", "Security_Concern")
    .otherwise(None)
)

display(df.select("CancellationCode", "cancellation_reason"))

# COMMAND ----------

display(df)

# COMMAND ----------

len(df.columns)

# COMMAND ----------



# COMMAND ----------

# day ==> 

# COMMAND ----------

# # create a test df using df
# df_test1=df.limit(20)
# display(df_test1)

# COMMAND ----------

# df_test2=df.limit(20)
# display(df_test2)

# COMMAND ----------

# # now we can union df_test1 and df_test2

# df_test3=df_test1.union(df_test2)

# display(df_test3)

# COMMAND ----------

# df_test3.count()

# COMMAND ----------

# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number

# window_spec = Window.partitionBy("FlightDate").orderBy("FlightDate")

# df_new = df_new.withColumn("row_num", row_number().over(window_spec))

# # display(df_new.select("FlightDate", "Flight_Number_Reporting_Airline", "row_num"))

# COMMAND ----------

# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number

# window_spec = Window.partitionBy("FlightDate","Flight_Number_Reporting_Airline").orderBy("FlightDate")

# df_new = df_new.withColumn("row_num", row_number().over(window_spec))

# display(
#     df_new.select("FlightDate", "Flight_Number_Reporting_Airline", "row_num")
# )

# COMMAND ----------

# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number

# window_spec = Window.partitionBy("FlightDate","Flight_Number_Reporting_Airline","Origin").orderBy("FlightDate")

# df_new = df_new.withColumn("row_num", row_number().over(window_spec))

# display(
#     df_new.select("FlightDate", "Flight_Number_Reporting_Airline","Origin","row_num"))

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp

# df_test3 = df_test3.withColumn("load_timestamp", current_timestamp())
# display(df_test3)

# COMMAND ----------

# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number

# window_spec = Window.partitionBy("FlightDate","Flight_Number_Reporting_Airline","Reporting_Airline","Origin","Dest").orderBy("load_timestamp")

# df_test3 = df_test3.withColumn("row_num", row_number().over(window_spec))

# display(df_test3.select("FlightDate", "Flight_Number_Reporting_Airline","Origin","Reporting_Airline","Dest","row_num"))

# COMMAND ----------

# # find duplicate row
# df_duplicates = df_test3.filter(col("row_num") > 1)

# print("total row :", df_test3.count())
# print("duplicate Rows :", df_duplicates.count())

# COMMAND ----------

df_silver=df

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema",True)\
    .save("s3://us-airline-data/airport-data/silver/")

# COMMAND ----------

df_silver = spark.read.format("delta").load("s3://us-airline-data/airport-data/silver/")


# COMMAND ----------

df_silver.columns

# COMMAND ----------

df_airport = spark.read.format("delta").load("s3://us-airline-data/airport-data/delta/")
df_airport.display()

# COMMAND ----------

# join on df_silver and df_airport baise on origine and iata

df_join = df_silver.join(
    df_airport,
    df_silver["Origin"] == df_airport["iata"],"left"
)
display(df_join)

# COMMAND ----------

# join in df_Silver and df_airport  using brodcast join
from pyspark.sql.functions import broadcast

df_join_b = df_silver.join(
    broadcast(df_airport),
    df_silver["Origin"] == df_airport["iata"],
    "left"
)

df_join_b.display()

# COMMAND ----------

df_join_b.count()

# COMMAND ----------

df_normal_join = df_silver.join(
    df_airport,
    df_silver["Origin"] == df_airport["iata"],
    "inner"
)

# COMMAND ----------

df_normal_join.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a UDF Function that Takes Flight Number as Input and Returns 1st, 4th and 7th Character

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# UDF to extract 1st, 4th and 7th character from flight number
# Flight_Number_Reporting_Airline is integer so convert to string first
def extract_flight_chars(flight_number):
    if flight_number is None:
        return None
    
    # Convert integer to string first
    flight_str = str(flight_number)
    result = ""
    
    if len(flight_str) >= 1:
        result += flight_str[0]
    if len(flight_str) >= 4:
        result += flight_str[3]
    if len(flight_str) >= 7:
        result += flight_str[6]
    return result

# Register as UDF
extract_flight_chars_udf = udf(extract_flight_chars, StringType())

# Apply on df_silver - no type cast needed as UDF handles it internally
df_silver = df_silver.withColumn(
    "flight_number_extract",
    extract_flight_chars_udf(col("Flight_Number_Reporting_Airline"))
)

# Verify
display(df_silver.select(
    "Flight_Number_Reporting_Airline",
    "flight_number_extract"
))

# COMMAND ----------

#  Create Origin Airport Coordinates Columns by Joining df_silver with df_airport on Origin and IATA Code and Drop Temporary Join Key Column origin_iata

# COMMAND ----------

from pyspark.sql.functions import broadcast, col

#  ADD THIS LINE (important fix)
df_silver = df_silver.drop("origin_lat", "origin_lon")

# Get origin coordinates from df_airport
df_origin_coords = df_airport.select(
    col("iata").alias("origin_iata"),
    col("lat").alias("origin_lat"),
    col("lon").alias("origin_lon")
)

# Join with df_silver on Origin == iata
df_silver = df_silver.join(
    broadcast(df_origin_coords),
    df_silver["Origin"] == df_origin_coords["origin_iata"],
    "left"
).drop("origin_iata")

# Verify
display(df_silver.select(
    "Origin",
    "origin_lat",
    "origin_lon"
).limit(10))

# COMMAND ----------

#  Create Destination Airport Coordinates Columns by Joining df_silver with df_airport on Dest and IATA Code and Drop Temporary Join Key Column dest_iata

# COMMAND ----------

from pyspark.sql.functions import broadcast, col

#  ADD THIS (important)
df_silver = df_silver.drop("dest_lat", "dest_lon")

# Get destination coordinates from df_airport
df_dest_coords = df_airport.select(
    col("iata").alias("dest_iata"),
    col("lat").alias("dest_lat"),
    col("lon").alias("dest_lon")
)

# Join with df_silver on Dest == iata
df_silver = df_silver.join(
    broadcast(df_dest_coords),
    df_silver["Dest"] == df_dest_coords["dest_iata"],
    "left"
).drop("dest_iata")

# Verify
display(df_silver.select(
    "Dest",
    "dest_lat",
    "dest_lon"
).limit(10))

# COMMAND ----------

# Create a Haversine Distance Function that Takes Origin Latitude, Origin Longitude, Destination Latitude and Destination Longitude as Parameters and Returns Distance in KM

# COMMAND ----------

from pyspark.sql.functions import acos, sin, cos, lit, col, round

PI = 3.141592653589793

def add_haversine_distance(df, lat1, lon1, lat2, lon2):
    return df.withColumn(
        "haversine_distance_km",
        round(   #  yaha add karo
            acos(
                sin(col(lat1) * lit(PI / 180)) * sin(col(lat2) * lit(PI / 180)) +
                cos(col(lat1) * lit(PI / 180)) * cos(col(lat2) * lit(PI / 180)) *
                cos(col(lon2) * lit(PI / 180) - col(lon1) * lit(PI / 180))
            ) * lit(6371.0),
            5   #  decimal ke baad 5 digits
        )
    )

df_silver = add_haversine_distance(
    df_silver,
    "origin_lat",
    "origin_lon",
    "dest_lat",
    "dest_lon"
)

display(df_silver.select(
    "Origin", "Dest",
    "haversine_distance_km"
).limit(10))

# COMMAND ----------

# Check total columns and column names
print("Total Columns:", len(df_silver.columns))
print()
print("Columns:", df_silver.columns)

# COMMAND ----------

# Save All  Columns to Delta Table in S3 with Partition by Year and Month and Enable Data Skipping on First 5 Indexed Columns for Faster Query Performance

# COMMAND ----------

silver_path = "s3://us-airline-data/delta/df_silver/"

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.dataSkippingNumIndexedCols", 5) \
    .partitionBy("year", "month") \
    .save(silver_path)

print("Total Columns Saved:", len(df_silver.columns))
print("Silver data saved successfully!")