import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
import datetime

print('Connecting to the postgresql database...')
con = psycopg2.connect(
    dbname='dvdrental',
    user='postgres',
    password='postgres',
    host='localhost',
    port='5432'
)
print('Done.')

print('Connecting to the MongoDB database...')
client = MongoClient()
client.drop_database('dvdrental')
db = client.dvdrental
print('Done.')


def insert_customers():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM customer')
    for row in cur:
        document = row
        document['_id'] = document.pop('customer_id')
        document['create_date'] = datetime.datetime.combine(document['create_date'], datetime.time.min)
        db['customer'].insert_one(document)


def insert_actors():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM actor')
    for row in cur:
        document = row
        document['_id'] = document.pop('actor_id')
        db['actor'].insert_one(document)


def insert_categories():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM category')
    for row in cur:
        document = row
        document['_id'] = document.pop('category_id')
        db['category'].insert_one(document)


def insert_films():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM film')
    for row in cur:
        document = row
        document['_id'] = document.pop('film_id')
        document['rental_rate'] = float(document['rental_rate'])
        document['replacement_cost'] = float(document['replacement_cost'])
        db['film'].insert_one(document)


def insert_film_actor():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM film_actor')
    for row in cur:
        document = row
        db['film_actor'].insert_one(document)


def insert_film_category():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM film_category')
    for row in cur:
        document = row
        db['film_category'].insert_one(document)


def insert_addresses():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM address')
    for row in cur:
        document = row
        document['_id'] = document.pop('address_id')
        db['address'].insert_one(document)


def insert_cities():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM city')
    for row in cur:
        document = row
        document['_id'] = document.pop('city_id')
        db['city'].insert_one(document)


def insert_countries():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM country')
    for row in cur:
        document = row
        document['_id'] = document.pop('country_id')
        db['country'].insert_one(document)


def insert_inventories():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM inventory')
    for row in cur:
        document = row
        document['_id'] = document.pop('inventory_id')
        db['inventory'].insert_one(document)


def insert_languages():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM language')
    for row in cur:
        document = row
        document['_id'] = document.pop('language_id')
        db['language'].insert_one(document)


def insert_payments():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM payment')
    for row in cur:
        document = row
        document['_id'] = document.pop('payment_id')
        document['amount'] = float(document['amount'])
        db['payment'].insert_one(document)


def insert_rentals():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM rental')
    for row in cur:
        document = row
        document['_id'] = document.pop('rental_id')
        db['rental'].insert_one(document)


def insert_rentals():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM rental')
    for row in cur:
        document = row
        document['_id'] = document.pop('rental_id')
        db['rental'].insert_one(document)


def insert_staff():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM staff')
    for row in cur:
        document = row
        document['_id'] = document.pop('staff_id')
        if document['picture']:
            document['picture'] = bytes(document['picture'])
        db['staff'].insert_one(document)


def insert_stores():
    cur = con.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM store')
    for row in cur:
        document = row
        document['_id'] = document.pop('store_id')
        db['store'].insert_one(document)


print('Creating collections...')

print('Inserting customers...')
insert_customers()
print('Inserting actors...')
insert_actors()
print('Inserting categories...')
insert_categories()
print('Inserting films...')
insert_films()
print('Inserting film_actor...')
insert_film_actor()
print('Inserting film_category...')
insert_film_category()
print('Inserting addresses...')
insert_addresses()
print('Inserting cities...')
insert_cities()
print('Inserting counties...')
insert_countries()
print('Inserting inventories...')
insert_inventories()
print('Inserting languages...')
insert_languages()
print('Inserting payments...')
insert_payments()
print('Inserting rentals...')
insert_rentals()
print('Inserting staff...')
insert_staff()
print('Inserting stores...')
insert_stores()


print('Done')

con.close()
