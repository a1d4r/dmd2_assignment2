from pymongo import MongoClient
import pandas as pd
import time

start_time = time.time()

print('Connecting to the MongoDB database...')
client = MongoClient()
db = client.dvdrental
print('Executing query...')

result = db.rental.aggregate([
    # join with inventory
    {
        "$lookup": {
            "from": "inventory",
            "localField": "inventory_id",
            "foreignField": "_id",
            "as": "inventory"
        }
    },
    {
        "$unwind": "$inventory"
    },
    # count the number of rentals for each film
    {
        "$group": {
            "_id": "$inventory.film_id",
            "number_of_rentals": {"$sum": 1}
        }
    },
    # join with films
    {
        "$lookup": {
            "from": "film",
            "localField": "_id",
            "foreignField": "_id",
            "as": "film"
        }
    },
    # join with film_category
    {
        "$lookup": {
            "from": "film_category",
            "localField": "_id",
            "foreignField": "film_id",
            "as": "category"
        }
    },
    # join with category
    {
        "$lookup": {
            "from": "category",
            "localField": "category.category_id",
            "foreignField": "_id",
            "as": "category"
        }
    },
    # leave only necessary fields
    {
        "$project": {
            "film": "$film.title",
            "category": "$category.name",
            "number_of_rentals": 1
        }
    },
    # extract from arrays
    {
        "$unwind": "$category"
    },
    {
        "$unwind": "$film"
    }
])

# save as pandas table
df = pd.DataFrame(list(result))
df = df[['_id', 'film', 'category', 'number_of_rentals']]

# sort by film_id
df = df.rename({'_id': 'film_id'}, axis='columns').sort_values(by='film_id')

# save the result to the csv file
df.to_csv('3.csv', index=False)

print('Done in {:.3f}s'.format(time.time() - start_time))
