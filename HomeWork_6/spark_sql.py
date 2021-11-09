from pyspark.sql import SparkSession
from pyspark.sql import *

from pyspark.sql.functions import *

spark = SparkSession.builder\
    .config('spark.driver.extraClassPath', '/home/pikarchu/drivers/postgresql-42.3.1.jar')\
    .master('local')\
    .appName('homework_6')\
    .getOrCreate()

spark

pg_url = 'jdbc:postgresql://192.168.0.110:5432/postgres'
pg_creds = {'user' : 'pguser', 'password' : 'secret'}

#First tasks
tb_film_category = spark.read.jdbc(pg_url, table='film_category', properties=pg_creds)
tb_category = spark.read.jdbc(pg_url, table='category', properties=pg_creds)

first_result = tb_film_category.join(tb_category , tb_film_category.category_id == tb_category.category_id, 'inner')\
    .groupby(tb_category.name)\
    .count()\
    .orderBy(desc('count'))

first_result.show()

#Second Query
tb_actor = spark.read.jdbc(pg_url, table='actor', properties=pg_creds)
tb_film_actor = spark.read.jdbc(pg_url, table='film_actor', properties=pg_creds)
tb_inventory = spark.read.jdbc(pg_url, table='inventory', properties=pg_creds)
tb_rental = spark.read.jdbc(pg_url, table='rental', properties=pg_creds)

second_result = tb_actor.join(tb_film_actor, tb_film_actor.actor_id == tb_actor.actor_id , 'inner')\
        .join(tb_inventory, tb_inventory.film_id == tb_film_actor.film_id , 'inner')\
        .join(tb_rental, tb_rental.inventory_id ==    tb_inventory.inventory_id   , 'inner')\
        .select(concat(col('first_name'), lit(' '), col('last_name')).alias('Actor_Name'))\
        .groupby('Actor_Name')\
        .count()\
        .orderBy(desc('count'))

second_result.show(10)

#Third Query

tb_payment =spark.read.jdbc(pg_url, table='payment', properties=pg_creds)
third_result= tb_category.join(tb_film_category,tb_film_category.category_id == tb_category.category_id , 'inner' )\
        .join(tb_inventory,tb_inventory.film_id  == tb_film_category.film_id  , 'inner' )\
        .join(tb_rental,tb_rental.inventory_id  == tb_inventory.inventory_id  , 'inner' )\
        .join(tb_payment,tb_payment.rental_id   == tb_rental.rental_id   , 'inner' )\
        .groupby(tb_category.name)\
        .agg(sum(tb_payment.amount))\
        .orderBy(desc('sum(amount)'))
third_result.show(1)

#Fourth Query

tb_film = spark.read.jdbc(pg_url, table='film', properties=pg_creds)
fourth_query = tb_film.join(tb_inventory, tb_inventory.film_id == tb_film.film_id, 'left_anti')
fourth_query.select('title').show()
fourth_query.count()


#fifth Query
windowSpec  = Window.partitionBy().orderBy(desc("count"))

fifth_query = tb_actor.join(tb_film_actor, tb_film_actor.actor_id == tb_actor.actor_id, 'inner')\
.join(tb_film_category, tb_film_category.film_id==tb_film_actor.film_id , 'inner')\
.join(tb_category, [tb_category.category_id ==tb_film_category.category_id , tb_category.name == 'Children'] , 'inner')\
.select(concat(col('first_name'), lit(' '), col('last_name')).alias('Actor_Name'))\
.groupby('Actor_Name')\
.count()\
.withColumn("rank",rank().over(windowSpec)) \
.where(col('rank') <= 3)


fifth_query.show()

#Sixth Query
tb_customer = spark.read.jdbc(pg_url, table='customer', properties=pg_creds)
tb_address = spark.read.jdbc(pg_url, table='address', properties=pg_creds)
tb_city = spark.read.jdbc(pg_url, table='city', properties=pg_creds)

Sixth_query = tb_city.join(tb_address, tb_address.city_id == tb_city.city_id, 'inner')\
    .join(tb_customer, tb_customer.address_id ==tb_address.address_id, 'inner')\
    .withColumn("not active", when(tb_customer.active == 1, 0).otherwise(1))\
    .groupby('city')\
    .agg({'active':'sum', 'not active':'sum'})\
    .sort(desc('sum(not active)'))

Sixth_query.show()

#seventh Query
windowSpec = Window.partitionBy(["city"]).orderBy(desc("sum(rental_duration)"))
seventh_query_1 = tb_film_category.join(tb_category, tb_film_category.category_id == tb_category.category_id, 'inner')\
    .join(tb_film, tb_film_category.film_id ==tb_film.film_id, 'inner')\
    .join(tb_inventory, tb_film.film_id ==tb_inventory.film_id, 'inner')\
    .join(tb_rental, tb_inventory.inventory_id ==tb_rental.inventory_id, 'inner')\
    .join(tb_customer, tb_rental.customer_id ==tb_customer.customer_id, 'inner')\
    .join(tb_address, tb_customer.address_id ==tb_address.address_id, 'inner')\
    .join(tb_city, [tb_address.city_id ==tb_city.city_id, ], 'inner')\
    .where((lower(tb_city.city).like('a%')) | (tb_city.city.like("%-%")))\
    .groupby([tb_city.city,tb_category.name])\
    .agg({'rental_duration':'sum'})\
    .withColumn("rnm", row_number().over(windowSpec))\
    .where(col('rnm') == 1)

seventh_query_1.show()
