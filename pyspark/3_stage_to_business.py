#!/usr/bin/python
from pyspark.sql import SparkSession

bucket = 'spark-final-project-bucket'
DATASET_SOURCE = 'final_dtlk_staging'
DATASET_TARGET = 'final_dtlk_business'
SOURCE_TABLES = [
    'Top_Actor',
    'Top_Category',
    'Top_Customer',
    'Top_City',
    'Top_Film',
    'Top_Store'
]
QUERY_MOVER = {
    'Top_Actor': 'SELECT last_name, COUNT(rental_id) AS Total_Rental FROM Top_Actor GROUP BY last_name ORDER BY Total_Rental DESC',
    'Top_Category': 'SELECT name AS Category, COUNT(rental_id) AS Total_Rental FROM Top_Category GROUP BY name ORDER BY Total_Rental DESC',
    'Top_Customer': 'SELECT last_name AS Customer, COUNT(rental_id) AS Total_Rental FROM Top_Customer GROUP BY last_name ORDER BY Total_Rental DESC',
    'Top_City': 'SELECT city, COUNT(rental_id) AS Total_Rental FROM Top_City GROUP BY city ORDER BY Total_Rental DESC',
    'Top_Film': 'SELECT title, COUNT(rental_id) AS Total_Rental FROM Top_Film GROUP BY title ORDER BY Total_Rental DESC',
    'Top_Store': 'SELECT store_id, COUNT(rental_id) AS Total_Rental FROM Top_Store GROUP BY store_id ORDER BY Total_Rental DESC'
}

spark = (
    SparkSession
    .builder
    .master('yarn')
    .appName('bigquery_stage_to_business')
    .getOrCreate()
)

spark.conf.set('temporaryGcsBucket', bucket)

for table in SOURCE_TABLES:
    print(table)
    spark_df = (
        spark
        .read
        .format('bigquery')
        .option('table', f'{DATASET_SOURCE}.{table}')
        .load()
    )
    spark_df.createOrReplaceTempView(table)


for query_name, query_str in QUERY_MOVER.items():
    # Apply business Convertions
    df_to_business = spark.sql(query_str)
    df_to_business.show(2)
    df_to_business.printSchema()
    # Spark Saving
    (
        df_to_business
        .write
        .format("bigquery")
        .mode("overwrite")
        .option("header", True)
        .option('table', f'{DATASET_TARGET}.{query_name}')
        .save()
    )
    print(f'Done with: {query_name}')
