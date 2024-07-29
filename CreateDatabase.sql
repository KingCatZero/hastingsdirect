CREATE OR REPLACE DATABASE HASTINGSDIRECT_STREETCRIME_DB;

CREATE STORAGE INTEGRATION HASTINGSDIRECT_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::386289825592:role/hastingsdirect_access'
    STORAGE_ALLOWED_LOCATIONS = ('*');

DESC INTEGRATION HASTINGSDIRECT_INT;

USE SCHEMA HASTINGSDIRECT_STREETCRIME_DB.LANDING;

CREATE OR REPLACE FILE FORMAT STREETCRIME_STAGE_FILEFORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = true
    COMPRESSION = gzip;

CREATE OR REPLACE STAGE STREETCRIME_STAGE
    STORAGE_INTEGRATION = HASTINGSDIRECT_INT
    URL = 's3://hastingsdirect-streetcrime/daily-dumps/'
    FILE_FORMAT = STREETCRIME_STAGE_FILEFORMAT;

LIST @STREETCRIME_STAGE;

CREATE SCHEMA LANDING;
CREATE SCHEMA FACT;

CREATE TABLE LANDING.CRIME (
    crime_category text
    ,crime_location_type text
    ,crime_context text
    ,crime_persistent_id text
    ,api_id int
    ,crime_location_subtype text
    ,crime_month text
    ,crime_latitude float
    ,crime_street_id int
    ,crime_street_name text
    ,crime_longitude float
    ,report_latitude float
    ,report_longitude float
    ,report_postcode text
    ,report_month text
);

CREATE TABLE FACT.CRIME (
    crime_category text
    ,crime_location_type text
    ,crime_context text
    ,crime_persistent_id text
    ,api_id int
    ,crime_location_subtype text
    ,crime_month text
    ,crime_latitude float
    ,crime_street_id int
    ,crime_street_name text
    ,crime_longitude float
    ,report_latitude float
    ,report_longitude float
    ,report_postcode text
    ,report_month text
) CLUSTER BY (
    report_month
);
