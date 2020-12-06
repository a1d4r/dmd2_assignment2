from pymongo import MongoClient
from collections import deque
import pandas as pd
import time

start_time = time.time()

print('Connecting to the MongoDB database...')
client = MongoClient()
db = client.dvdrental
print('Executing query...')

# actor for which we find the degree of separation
selected_actor_id = 1

# list of ids of all the actors in the table
actors = list(db.actor.aggregate([
    {
        "$project": {"name": {"$concat": ["$first_name", " ", "$last_name"]}}
    }
]))

actor_ids = list(map(lambda x: x['_id'], actors))

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

# Adjacency list for actors
adj = {actor_id: [] for actor_id in actor_ids}

# Fill the adjacency list
for actor1_id in actor_ids:
    for actor2_id in actor_ids:
        if len(set(actor_films[actor1_id]) & set(actor_films[actor2_id])) > 0:
            adj[actor1_id].append(actor2_id)

# breath-first search for calculating the degree of separation
visited = {actor_id: False for actor_id in actor_ids}
queue = deque([selected_actor_id])
degree = {actor_id: -1 for actor_id in actor_ids}
while queue:
    actor_id = queue.popleft()
    for actor2_id in adj[actor_id]:
        if degree[actor2_id] == -1:
            degree[actor2_id] = degree[actor_id] + 1
            queue.append(actor2_id)

# building the result table
result = [{"_id": actor["_id"], "name": actor["name"], "degree_of_separation": degree[actor["_id"]]} for actor in
          actors]

# save the result to the csv file
df = pd.DataFrame(result).rename({'_id': 'actor_id'}, axis='columns')
df.to_csv('5.csv', index=False)

print('Done in {:.3f}s'.format(time.time() - start_time))
