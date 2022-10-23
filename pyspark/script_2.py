

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import sys
print('IMPORT BIBLIOTECAS')
print('***------ DONE')


print('***------ SPARK SESSION... ------***')
spark = SparkSession.builder.appName('SparkApplicationB').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
print('***------ DONE')


print('***------ SETTING VARIABLES... ------***')
GCS_URI_INPUT = 'gs://taller-airflow-amesa/pyspark/data/retail_2021.csv'
GCS_URI_OUTPUT = 'gs://taller-airflow-amesa/pyspark/pyspark-output/{}/retail_2021_output.csv'.format(
    sys.argv[1])
print('***------ DONE')


print('***------ LECTURA DESDE GCS... ------***')
retail_df = (
    spark
    .read
    .option('header', 'true')
    .option('inferSchema', value=True)
    .option('delimiter', ';')
    .csv('{}'.format(GCS_URI_INPUT))
)
print('***------ DONE')


print('***------ SCHEMA ------***')
print(json.dumps(json.loads(retail_df._jdf.schema().json()), indent=4, sort_keys=True))
print('***------ DONE')


print('***------ COLUMNAS SELECCIONADAS ------***')
retail_df.select('quant', 'uprice_usd', 'total_inc').show(15)
print('***------ DONE')


print('***------ SPARK SQL TRANSFORMATION... ------***')
retail_df.createOrReplaceTempView('retail_years')
avg_variables = (
    retail_df
    .groupBy('year', 'area')
    .agg(
        F.avg('quant').alias('avg_quant'),
        F.avg('uprice_usd').alias('avg_uprice_usd'),
        F.avg('total_inc').alias('avg_total_inc')
    )
    .orderBy(F.col('avg_quant').desc())
)
print('***------ DONE')


print('***------ COLUMNAS AGRUPADAS ------***')
avg_variables.show(5)
print('***------ DONE')


print('***------ GUARDADO EN GCS... ------***')
(
    avg_variables
    .write
    .option('header', True)
    .mode('overwrite')
    .csv('{}'.format(GCS_URI_OUTPUT))
)
print('***------ DONE')

spark.stop()
