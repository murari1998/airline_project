drop database flight_analysis;
// createing database name as flight_analysis
create or replace database flight_analysis;

// use database 
use database flight_analysis;

-- // creating schema 
-- create or replace schema flight_analysis.staging;

-- CREATE STORAGE INTEGRATION s3_flight_gold_int
-- TYPE = EXTERNAL_STAGE
-- STORAGE_PROVIDER = S3
-- ENABLED = TRUE
-- STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::512494867021:role/murari'
-- STORAGE_ALLOWED_LOCATIONS = ('s3://us-airline-data/gold/');

-- DESC INTEGRATION s3_flight_gold_int;


// createing file format
CREATE OR REPLACE FILE FORMAT parquet_f
TYPE = PARQUET;

// 
CREATE OR REPLACE STAGE gold_stage_s3
URL = 's3://us-airline-data/gold/'
CREDENTIALS = (
    AWS_KEY_ID = ''
    AWS_SECRET_KEY = ''
)
FILE_FORMAT = parquet_f;


LIST @gold_stage_s3;


// creating table for airline_api delta table  as MONTLY AIRLINE API

CREATE OR REPLACE TABLE MONTHLY_AIRLINE_API (
    flight_year NUMBER,
    flight_month NUMBER,
    airline_code STRING,
    reporting_airline STRING,
    total_number_of_flights NUMBER,
    total_delayed_flights NUMBER,
    total_flights_cancelled NUMBER,
    total_flights_diverted NUMBER,
    avg_arrival_delay_minutes FLOAT,
    on_time_flight_percentage FLOAT,
    non_cancelled_flight_percentage FLOAT,
    cancelled_flight_percentage FLOAT,
    median_arrival_delay FLOAT,
    avg_carrier_delay FLOAT,
    avg_weather_delay FLOAT,
    avg_security_delay FLOAT,
    avg_late_aircraft_delay FLOAT,
    avg_distance_travelled FLOAT,
    total_distance_travelled FLOAT,
    monthly_rank NUMBER
);

// set retention time 
alter table MONTHLY_AIRLINE_API
set DATA_RETENTION_TIME_IN_DAYS = 10;


// check retaintion time of this table
SHOW TABLES LIKE 'MONTHLY_AIRLINE_API';



// creting a new table name as 

CREATE OR REPLACE TABLE ANNUAL_ROUTE_PERFORMANCE (
    flight_year NUMBER,
    route STRING,
    origin_code STRING,
    destination_code STRING,
    number_of_flights NUMBER,
    avg_arrival_delay FLOAT,
    avg_distance_travelled FLOAT,
    total_delayed_flights NUMBER,
    number_of_airlines_on_route NUMBER,
    on_time_percentage_airline_percentage FLOAT
);


// set retention time 
alter table ANNUAL_ROUTE_PERFORMANCE
set DATA_RETENTION_TIME_IN_DAYS = 10;

// check retaintion time of ANNUAL_ROUTE_PERFORMANCE table

SHOW TABLES LIKE 'ANNUAL_ROUTE_PERFORMANCE';


// creating 3rd table airport_departure_kpi
CREATE OR REPLACE TABLE AIRPORT_DEPARTURE_KPI (
    flight_year NUMBER,
    flight_month NUMBER,
    origin_code STRING,
    name STRING,
    city STRING,
    state STRING,
    lon FLOAT,
    lat FLOAT,
    total_departure NUMBER,
    total_cancelled_departure NUMBER,
    avg_delayed_departure FLOAT,
    avg_route_distance FLOAT,
    number_of_flights_operating NUMBER,
    avg_airtime FLOAT,
    departure_on_time_percentage FLOAT,
    year_month STRING
);


// set retention time 
alter table AIRPORT_DEPARTURE_KPI
set DATA_RETENTION_TIME_IN_DAYS = 10;


// CHECK  retaintion time ANNUAL_ROUTE_PERFORMANCE'

SHOW TABLES LIKE 'AIRPORT_DEPARTURE_KPI';

// creating table delay_cause_table

CREATE OR REPLACE TABLE DELAY_CAUSE_TABLE (
    flight_year NUMBER,
    flight_month NUMBER,
    airline_code STRING,
    total_minutes_delayed FLOAT,
    total_weather_delayed_minutes FLOAT,
    total_carrier_delayed_minutes FLOAT,
    total_security_delayed_minutes FLOAT,
    total_late_aircraft_delayed_minutes FLOAT,
    weather_delay_percentage FLOAT,
    carrier_delay_percentage FLOAT,
    security_delay_percentage FLOAT,
    late_aircraft_delay_percentage FLOAT
);

// set retention time 
alter table DELAY_CAUSE_TABLE
set DATA_RETENTION_TIME_IN_DAYS = 10;


// // CHECK  retaintion time ANNUAL_ROUTE_PERFORMANCE'

SHOW TABLES LIKE 'DELAY_CAUSE_TABLE';




-- now creating pipe on table montly_airline_api

CREATE OR REPLACE PIPE PIPE_MONTHLY_AIRLINE_API
AUTO_INGEST = TRUE
AS
COPY INTO MONTHLY_AIRLINE_API
FROM @gold_stage_s3/airline_api/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


// desc pipe 

DESC PIPE PIPE_MONTHLY_AIRLINE_API;



// check ststus 
SELECT SYSTEM$PIPE_STATUS('PIPE_MONTHLY_AIRLINE_API');

// refresh pipe (load exxisting file)
ALTER PIPE PIPE_MONTHLY_AIRLINE_API REFRESH;


// show table 
select * from MONTHLY_AIRLINE_API;

// count total number of row
SELECT COUNT(*) AS monthly_airline_api_count FROM MONTHLY_AIRLINE_API;


// -- now creating pipe on table ANNUAL_ROUTE_PERFORMANCE


CREATE OR REPLACE PIPE PIPE_ANNUAL_ROUTE_PERFORMANCE
AUTO_INGEST = TRUE
AS
COPY INTO ANNUAL_ROUTE_PERFORMANCE
FROM @gold_stage_s3/df_gold_route_kpi/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


// desc pipe 

DESC PIPE PIPE_ANNUAL_ROUTE_PERFORMANCE;

// check ststus 
SELECT SYSTEM$PIPE_STATUS('PIPE_ANNUAL_ROUTE_PERFORMANCE');


// refresh pipe (load exxisting file)
ALTER PIPE PIPE_ANNUAL_ROUTE_PERFORMANCE REFRESH;


select * from ANNUAL_ROUTE_PERFORMANCE;


// count total num of row 
SELECT COUNT(*) AS monthly_airline_api_count FROM ANNUAL_ROUTE_PERFORMANCE;




// creating pipe on 3rd table
CREATE OR REPLACE PIPE PIPE_AIRPORT_DEPARTURE_KPI
AUTO_INGEST = TRUE
AS
COPY INTO AIRPORT_DEPARTURE_KPI
FROM @gold_stage_s3/df_gold_airport_departure_kpi/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;



// refresh pipe (load exxisting file)
ALTER PIPE PIPE_AIRPORT_DEPARTURE_KPI REFRESH;


select * from AIRPORT_DEPARTURE_KPI ;


// count total num of row 
SELECT COUNT(*) AS monthly_airline_api_count FROM AIRPORT_DEPARTURE_KPI;




// creating pipe on 4th table

CREATE OR REPLACE PIPE PIPE_DELAY_CAUSE_TABLE
AUTO_INGEST = TRUE
AS
COPY INTO DELAY_CAUSE_TABLE
FROM @gold_stage_s3/delay_cause_table/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;




// refresh pipe (load exxisting file)
ALTER PIPE PIPE_DELAY_CAUSE_TABLE REFRESH;



select * from DELAY_CAUSE_TABLE;



// count total num of row 
SELECT COUNT(*) AS monthly_airline_api_count FROM DELAY_CAUSE_TABLE;


// show all tables
select * from FLIGHT_ANALYSIS.STAGING.MONTHLY_AIRLINE_API;
select * from FLIGHT_ANALYSIS.STAGING.ANNUAL_ROUTE_PERFORMANCE;
select * from FLIGHT_ANALYSIS.STAGING.AIRPORT_DEPARTURE_KPI;
select * from FLIGHT_ANALYSIS.STAGING.DELAY_CAUSE_TABLE;



-- -------################   dyas= 2    ##############---------------------------------------------------------------------------------

// using databaase
USE DATABASE flight_analysis;

// creating a new schema
CREATE SCHEMA IF NOT EXISTS dimensions;
USE SCHEMA dimensions;


-- creating  AIRLINE DIMENSION TABLE (SCD TYPE 2)

CREATE OR REPLACE TABLE airline_dimension (
    airline_code VARCHAR,
    airline_name VARCHAR,
    airport_name VARCHAR,
    carrier_plane VARCHAR,
    is_carrier_flight BOOLEAN,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN
);


-- STAGING TABLE (FROM df_silver / source)

CREATE OR REPLACE TEMP TABLE airline_stage AS
SELECT DISTINCT 
    m.airline_code,

    CASE m.airline_code
        WHEN 'F9' THEN 'Frontier Airlines'
        WHEN 'YV' THEN 'Mesa Airlines'
        WHEN 'AA' THEN 'American Airlines'
        WHEN 'NK' THEN 'Spirit Airlines'
        WHEN 'OH' THEN 'PSA Airlines'
        WHEN 'YX' THEN 'Republic Airways'
        WHEN 'AS' THEN 'Alaska Airlines'
        WHEN 'MQ' THEN 'Envoy Air'
        WHEN 'DL' THEN 'Delta Air Lines'
        WHEN 'UA' THEN 'United Airlines'
        WHEN '9E' THEN 'Endeavor Air'
        WHEN 'HA' THEN 'Hawaiian Airlines'
        WHEN 'QX' THEN 'Horizon Air'
        WHEN 'OO' THEN 'SkyWest Airlines'
        WHEN 'WN' THEN 'Southwest Airlines'
        WHEN 'B6' THEN 'JetBlue Airways'
        WHEN 'G4' THEN 'Allegiant Air'
        ELSE 'Unknown Airline'
    END AS airline_name,

    a.name AS airport_name,
    NULL AS carrier_plane,

    TRUE AS is_carrier_flight,
    CURRENT_DATE() AS effective_start_date

FROM flight_analysis.staging.MONTHLY_AIRLINE_API m
LEFT JOIN flight_analysis.staging.AIRPORT_DEPARTURE_KPI a
ON m.flight_year = a.flight_year
AND m.flight_month = a.flight_month;

--  INITIAL LOAD (FIRST TIME ONLY)

INSERT INTO airline_dimension
SELECT 
    airline_code,
    airline_name,
    airport_name,
    carrier_plane,
    is_carrier_flight,
    effective_start_date,
    NULL,
    TRUE
FROM airline_stage;

--  SCD TYPE 2 IMPLEMENTATION


--  Expire old records
UPDATE airline_dimension tgt
SET 
    effective_end_date = CURRENT_DATE(),
    is_current = FALSE
FROM airline_stage src
WHERE tgt.airline_code = src.airline_code
AND tgt.is_current = TRUE
AND tgt.airline_name != src.airline_name;

--  Insert new records
INSERT INTO airline_dimension
SELECT 
    src.airline_code,
    src.airline_name,
    src.airport_name,
    src.carrier_plane,
    src.is_carrier_flight,
    CURRENT_DATE(),
    NULL,
    TRUE
FROM airline_stage src
LEFT JOIN airline_dimension tgt
ON src.airline_code = tgt.airline_code
AND tgt.is_current = TRUE
WHERE tgt.airline_code IS NULL
   OR tgt.airline_name != src.airline_name;

-- VERIFY DATA

SELECT * FROM airline_dimension;


--  CTE: GENERATE NUMBERS 1 TO 100

WITH numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS num
    FROM TABLE(GENERATOR(ROWCOUNT => 100))
)
SELECT * FROM numbers;


--  CREATE DATES: MAY 1 TO MAY 7

WITH may_dates AS (
    SELECT DATEADD(DAY, SEQ4(), '2023-05-01') AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 7))
)
SELECT * FROM may_dates;

--  DATE DIMENSION TABLE (750 DAYS: 2021–2022)

CREATE OR REPLACE TABLE date_dimension AS
SELECT
    generated_date AS full_date,

    YEAR(generated_date) AS year_number,
    MONTH(generated_date) AS month_number,
    MONTHNAME(generated_date) AS month_name,

    DAYOFWEEKISO(generated_date) AS day_of_week_number,
    DAYNAME(generated_date) AS day_of_week_name,

    QUARTER(generated_date) AS quarter_number,

    -- Weekend flag
    CASE 
        WHEN DAYOFWEEKISO(generated_date) IN (6,7) THEN TRUE
        ELSE FALSE
    END AS weekend_flag,

    -- Season logic
    CASE 
        WHEN MONTH(generated_date) IN (12,1,2) THEN 'Winter'
        WHEN MONTH(generated_date) IN (3,4) THEN 'Spring'
        WHEN MONTH(generated_date) IN (5,6,7) THEN 'Summer'
        ELSE 'Monsoon'
    END AS season_weather,

    -- Year-Month
    TO_CHAR(generated_date, 'YYYY-MM') AS year_month

FROM (
    SELECT DATEADD(DAY, SEQ4(), '2021-01-01') AS generated_date
    FROM TABLE(GENERATOR(ROWCOUNT => 750))
)
WHERE generated_date < '2023-01-01';


-- 10. VERIFY DATE DIMENSION

SELECT * FROM date_dimension;



SELECT * FROM airline_dimension;




-- <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<days >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


  

select  