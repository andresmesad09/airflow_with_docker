

import sys
from pyspark.sql import SparkSession
print('Import libraries')
print('***------ DONE')

spark = SparkSession.builder.appName('SparkApplicationA').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

print(sys.version)
print('message:', sys.argv[1], sys.argv[2])
print('***------ DONE')

spark.stop()
