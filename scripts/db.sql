DROP DATABASE IF EXISTS project;
CREATE DATABASE project;
\c project;


CREATE TABLE test (
id VARCHAR (50) NOT NULL PRIMARY KEY,
vendor_id integer,
pickup_datetime date,
passenger_count integer,
pickup_longitude float,
pickup_latitude float,
dropoff_longitude float,
dropoff_latitude float,
store_and_fwd_flag VARCHAR (50)
);

CREATE TABLE train (
id VARCHAR (50) NOT NULL PRIMARY KEY,
vendor_id integer,
pickup_datetime date,
dropoff_datetime date,
passenger_count integer,
pickup_longitude float,
pickup_latitude float,
dropoff_longitude float,
dropoff_latitude float,
store_and_fwd_flag VARCHAR (50),
trip_duration integer
);

\COPY test FROM 'data/test.csv' DELIMITER ',' CSV HEADER NULL AS 'null';
\COPY train FROM 'data/train.csv' DELIMITER ',' CSV HEADER NULL AS 'null';