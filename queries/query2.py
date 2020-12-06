from pymongo import MongoClient
import pandas as pd
import time

start_time = time.time()

print('Connecting to the MongoDB database...')
client = MongoClient()
db = client.dvdrental
print('Executing query...')

# list of ids of all the actors in the table
actors = list(db.actor.aggregate([
    {
        "$project": {"name": {"$concat": ["$first_name", " ", "$last_name"]}}
    }
]))

actor_ids = list(map(lambda x: x['_id'], actors))
actor_names = list(map(lambda x: x['name'], actors))

# films for each actor
actor_films = list(db.film_actor.aggregate([
    {
        "$group": {
            "_id": "$actor_id",
            "films": {"$push": "$film_id"}
        }
    }
]))

actor_films = {x["_id"]: x["films"] for x in actor_films}

# Add empty lists for actors who has no movies with them in the collection
for actor in actors:
    if actor["_id"] not in actor_films:
        actor_films["_id"] = []

# Resulting table
df = pd.DataFrame(columns=actor_ids, index=actor_ids)

# Fill the table
for actor1 in actors:
    row = {}
    for actor2 in actors:
        # For each pair of actors find the number of films they co-starred in
        row[actor2["_id"]] = len(set(actor_films[actor1["_id"]]) & set(actor_films[actor2["_id"]]))
    df.loc[actor1["_id"]] = pd.Series(row)

# id-name dictionary to convert ids to names
id_name = {x["_id"]: x["name"] for x in actors}

# save as pandas table
df = df.rename(columns=id_name, index=id_name)

# save the result to the csv file
df.to_csv('2.csv')

print('Done in {:.3f}s'.format(time.time() - start_time))
