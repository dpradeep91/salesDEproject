from os.path import join, abspath, expanduser
from pyspark.sql import SparkSession
# from pyspark.sql.function import from_json

warehouse_location=abspath('spark-warehouse')

spark=SparkSession.builder.appName('Feature Processing').config('spark.sql.warehouse.dir',warehouse_location).enableHiveSupport().getOrCreate()

# df=spark.read.format('csv').option('header','true').load('hdfs://namenode/features/features_combined.csv')
df=spark.read.csv('hdfs://namenode:9000/features/features_combined.csv')

# df.createOrReplaceTempView("sampleView")

df.write.mode('append').option('header', 'true').insertInto('feature_data')

