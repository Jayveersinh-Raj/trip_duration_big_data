#!/bin/bash

# Run the db.sql file using the psql command
psql -U postgres -d project -f sql/db.sql

# Run the sqoop command to import all tables
sqoop import-all-tables \
  -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
  -Dmapreduce.job.user.classpath.first=true \
  --connect jdbc:postgresql://localhost/project \
  --username postgres \
  --warehouse-dir /project \
  --as-avrodatafile \
  --compression-codec=snappy \
  --outdir /project/avsc \
  --m 1