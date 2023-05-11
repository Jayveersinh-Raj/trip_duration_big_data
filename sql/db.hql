USE projectdb;
DROP TABLE test;
DROP TABLE train;
DROP DATABASE IF EXISTS projectdb;


CREATE DATABASE projectdb;
USE projectdb;

SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

CREATE EXTERNAL TABLE test STORED AS AVRO LOCATION '/project/test' TBLPROPERTIES ('avro.schema.url'='/project/avsc/test.avsc');
CREATE EXTERNAL TABLE train STORED AS AVRO LOCATION '/project/train' TBLPROPERTIES ('avro.schema.url'='/project/avsc/train.avsc');