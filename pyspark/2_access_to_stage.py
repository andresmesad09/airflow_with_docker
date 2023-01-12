#!/usr/bin/python
from pyspark.sql import SparkSession

bucket = 'spark-final-project-bucket'
DATASET_SOURCE = 'final_dtlk_access'
DATASET_TARGET = 'final_dtlk_staging'
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
    'Top_Actor': '''
    SELECT last_name, rental_id, customer_id
    FROM actor
    INNER JOIN (
        SELECT actor_id, rental_id, customer_id
        FROM film_actor
        INNER JOIN (
            SELECT rental.rental_id,inventory.inventory_id, rental.customer_id, film_id
            FROM rental
            INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
            ) AS A
            ON film_actor.film_id = A.film_id
            ) AS B
            ON actor.actor_id = B.actor_id
    ''',
    'Top_Category': '''
    SELECT name, rental_id, customer_id
    FROM category
    INNER JOIN (
        SELECT category_id, rental_id, customer_id
        FROM film_category
        INNER JOIN (
            SELECT rental.rental_id, inventory.inventory_id, rental.customer_id, film_id
            FROM rental
            INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
            ) AS A
            ON film_category.film_id = A.film_id
            ) AS B
            ON category.category_id = B.category_id
    ''',
    'Top_Customer': '''
    SELECT last_name, store_id, rental_id, amount
    FROM payment
    INNER JOIN customer
    ON payment.customer_id = customer.customer_id''',
    'Top_City': '''
    SELECT city, rental_id, amount
    FROM city
    INNER JOIN (
        SELECT city_id, rental_id, amount
        FROM address
        INNER JOIN (
            SELECT last_name, store_id, rental_id, amount, address_id
            FROM payment
            INNER JOIN customer
            ON payment.customer_id = customer.customer_id
            ) AS A
            ON address.address_id = A.address_id
            ) AS B
            ON city.city_id = B.city_id
    ''',
    'Top_Film': '''
    SELECT title, release_year, rating, rental_id
    FROM film
    INNER JOIN (
        SELECT film_id, rental_id
        FROM inventory
        INNER JOIN rental
        ON inventory.inventory_id = rental.inventory_id
        ) AS A
        ON film.film_id = A.film_id''',
    'Top_Store': '''
    SELECT store.store_id, rental_id
    FROM store
    INNER JOIN (
        SELECT store_id, rental_id
        FROM inventory
        INNER JOIN rental
        ON inventory.inventory_id = rental.inventory_id
        ) AS A
        ON store.store_id = A.store_id
    ''',
}

spark = (
    SparkSession
    .builder
    .master('yarn')
    .appName('bigquery_access_to_stage')
    .getOrCreate()
)
spark.conf.set('temporaryGcsBucket', bucket)

# Import Tables
for table in SOURCE_TABLES:
    # Read Tables
    spark_df = (
        spark
        .read
        .format("bigquery")
        .option("header", "true")
        .option("inferSchema", "true")
        .option('table', f'{DATASET_SOURCE}.{table}')
        .load()
    )
    # Create a Temp View
    spark_df.createOrReplaceTempView(f'{table}')
    print(f' Temp View Imported: {table}')

for query_name, query_str in QUERY_MOVER.items():
    # Apply Stage Convertions
    df_to_stage = spark.sql(query_str)
    df_to_stage.show(2)
    df_to_stage.printSchema()
    # Spark Saving
    (
        df_to_stage
        .write
        .format("bigquery")
        .mode("overwrite")
        .option("header", True)
        .option('table', f'{DATASET_TARGET}.{query_name}')
        .save()
    )
    print(f'Done with: {query_name}')
