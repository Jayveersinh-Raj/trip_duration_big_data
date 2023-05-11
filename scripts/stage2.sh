hdfs dfs -mkdir -p /project/avsc
hdfs dfs -put /project/avsc/test.avsc /project/avsc
hdfs dfs -put /project/avsc/train.avsc /project/avsc

hive -f sql/db.hql

