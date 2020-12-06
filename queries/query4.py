from pymongo import MongoClient
from scipy import spatial
import pandas as pd
import time

start_time = time.time()

print('Connecting to the MongoDB database...')
client = MongoClient()
db = client.dvdrental
print('Executing query...')

# the customer for which we make recommendations
selected_customer_id = 1

# get ids of all customers
customers = list(db.customer.find({}, {"_id": 1}))
customer_ids = [customer["_id"] for customer in customers]
customer_ids.sort()

# get ids of all films
films = list(db.film.find({}, {"_id": 1, "title": 1}))
film_ids = [film["_id"] for film in films]
film_ids.sort()

# the matrix rating_matrix[user_id][film_id] = 1 if user_id has seen film_id
rating_matrix = {customer_id: {film_id: 0 for film_id in film_ids} for customer_id in customer_ids}


# iterate over all customer-film pairs
for row in db.rental.aggregate([
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
    # leave only customer_id and film_id
    {
        "$project": {
            "customer_id": 1,
            "film_id": "$inventory.film_id"
        }
    }
]):
    rating_matrix[row["customer_id"]][row["film_id"]] = 1

# Calculating user similarities using Jaccard similarity index
# It is more appropriate because we have seen/not seen instead of user ratings
# https://realpython.com/build-recommendation-engine-collaborative-filtering/

# user arrays
user_array = {customer_id: list(rating_matrix[customer_id].values()) for customer_id in customer_ids}
# user similarities with the selected user
user_similarity = {customer_id: 1 - spatial.distance.jaccard(user_array[selected_customer_id], user_array[customer_id])
                   for customer_id in customer_ids}

# how much similar users we consider for recommendations
n = min(20, len(customer_ids) - 1)

# list of n most similar users in the form (customer_id, similarity)
most_similar_users = sorted(user_similarity.items(), key=lambda x: x[1])[-2:-n-2:-1]

# estimation for each film and for selected customer based on n most similar customers
film_estimation = {film_id: 0.0 for film_id in film_ids}

# calculating weighted average for each film
sum_of_weights = 0

for film_id in film_ids:
    for i in range(n):
        customer_id, similarity = most_similar_users[i]
        sum_of_weights += similarity
        film_estimation[film_id] += rating_matrix[customer_id][film_id] * similarity

for film_id in film_ids:
    film_estimation[film_id] /= sum_of_weights

# for numbers to look prettier
for film_id in film_ids:
    film_estimation[film_id] = round(film_estimation[film_id] * 10000, 3)

# building the result table
result = [{"film_id": film["_id"], "film_title": film["title"], "film_estimation": film_estimation[film["_id"]]}
          for film in films]

# save as pandas table
df = pd.DataFrame(result)

# sort by film estimation
df = df.sort_values(by='film_estimation', ascending=False)

# save the result to the csv file
df.to_csv('4.csv', index=False)

print('Done in {:.3f}s'.format(time.time() - start_time))
