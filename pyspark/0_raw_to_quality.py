#!/usr/bin/python
from pyspark.sql import SparkSession

bucket = 'spark-final-project-bucket'
DATASET_SOURCE = 'final_dtlk_raw'
DATASET_TARGET = 'final_dtlk_quality'
SOURCE_TABLES = [
    'actor',
    'address',
    'category',
    'city',
    'country',
    'customer',
    'film',
    'film_actor',
    'film_category',
    'inventory',
    'language',
    'payment',
    'rental',
    'staff',
    'store'
]
QUERY_MOVER = {
    'actor': 'SELECT * FROM actor',
    'address': 'SELECT address_id, address, district, city_id, postal_code, phone, last_update FROM address',
    'category': 'SELECT * FROM category',
    'city': 'SELECT * FROM city',
    'country': 'SELECT * FROM country',
    'customer': 'SELECT * FROM customer',
    'film': 'SELECT * FROM film',
    'film_actor': 'SELECT * FROM film_actor',
    'film_category': 'SELECT * FROM film_category',
    'inventory': 'SELECT * FROM inventory',
    'language': 'SELECT * FROM language',
    'payment': 'SELECT * FROM payment',
    'rental': 'SELECT * FROM rental',
    'staff': 'SELECT * FROM staff',
    'store': 'SELECT * FROM store',
}

spark = (
    SparkSession
    .builder
    .master('yarn')
    .appName('bigquery_raw_to_quality')
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

    # actor is a pretty clean table, we could just move it to qty
    df_to_quality = spark.sql(QUERY_MOVER[table])

    df_to_quality.show()
    df_to_quality.printSchema()

    # Spark saving
    (
        df_to_quality.write.format('bigquery')
        .option('table', f'{DATASET_TARGET}.{table}')
        .mode('overwrite')
        .save()
    )
    print(f"Done with: {table}")
