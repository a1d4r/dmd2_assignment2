from pymongo import MongoClient
import pandas as pd
import time

start_time = time.time()

print('Connecting to the MongoDB database...')
client = MongoClient()
db = client.dvdrental
print('Executing query...')

current_year = db.rental.aggregate([
    # the latest rental date
    {
        "$group": {
            "_id": "null",
            "latest_date": {"$max": "$rental_date"}
        }
    },
    # leave only
    {
        "$project": {
            "_id": 0,
            "current_year": {"$year": "$latest_date"}
        }
    }
]).next()["current_year"]

result = db.rental.aggregate([
    # add rental_year for each document
    {
        "$addFields": {
            "rental_year": {"$year": "$rental_date"}
        }
    },
    # all the rentals for the current year
    {
        "$match": {
            "rental_year": current_year
        }
    },
    # leave only customer_id and inventory_id
    {
        "$project": {
            "customer_id": 1,
            "inventory_id": 1
        }
    },
    # join with inventory
    {
        "$lookup": {
            "from": "inventory",
            "localField": "inventory_id",
            "foreignField": "_id",
            "as": "inventory"
        }
    },
    # join with film
    {
        "$lookup": {
            "from": "film_category",
            "localField": "inventory.film_id",
            "foreignField": "film_id",
            "as": "category"
        }
    },
    # leave only customer_id and category_id
    {
        "$project": {
            "category_id": "$category.category_id",
            "customer_id": "$customer_id",
        }
    },
    # group categories by customer_id
    {
        "$group": {
            "_id": "$customer_id",
            "categories": {"$addToSet": "$category_id"}
        }
    },
    # leave only those customers, who has more than 1 category
    {
        "$match": {
            "categories.1": {"$exists": True}
        }
    },
    # join with customer
    {
        "$lookup": {
            "from": "customer",
            "localField": "_id",
            "foreignField": "_id",
            "as": "customer"
        }
    },
    {
        "$unwind": "$customer"
    },
    # leave only customers' names
    {
        "$project": {
            "first_name": "$customer.first_name",
            "last_name": "$customer.last_name"
        }
    }
])

# save as pandas table
df = pd.DataFrame(list(result))

# sort by customer_id
df = df.rename({'_id': 'customer_id'}, axis='columns').sort_values(by='customer_id')

# save the result to the csv file
df.to_csv('1.csv', index=False)

print('Done in {:.3f}s'.format(time.time() - start_time))
