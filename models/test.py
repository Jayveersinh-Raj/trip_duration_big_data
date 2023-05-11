from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("BDT Project")\
        .config("spark.sql.catalogImplementation","hive")\
        .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()



train = spark.read.format("avro").table('projectdb.train')
train.createOrReplaceTempView('train')

test = spark.read.format("avro").table('projectdb.test')
test.createOrReplaceTempView('test')

train.write.format('csv').option('header', 'true').mode('overwrite').option('path', '/Data/train.csv').save()
test.write.format('csv').option('header', 'true').mode('overwrite').option('path', '/Data/test.csv').save()

spark.stop()