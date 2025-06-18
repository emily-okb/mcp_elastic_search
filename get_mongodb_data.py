from pymongo import MongoClient
import json
from tqdm import tqdm

# Replace with your real connection string
connection_string = "mongodb://mongo.web.web:lQ0ecPUy3u871ECbgsMc5IM0ceiYuHyI@web.mongo.nb.com:27017/admin?retryWrites=true&replicaSet=web&readPreference=secondaryPreferred&connectTimeoutMS=10000&authSource=admin"
client = MongoClient(connection_string)

# Select database and collection
db = client["mcp"]
collection = db["mcp_server"]

field_ids = ["about", "tools"]

projection = {field: 1 for field in field_ids}
#projection["_id"] = 0

results = collection.find({}, projection)


for doc in tqdm(results):
    data = {"id": doc["_id"],
            "about": doc.get("about", ""),
            "tools": doc.get("tools", [])
            }
    _, mcp_id = doc["_id"].split('/',1)

    with open(f"mcp_data/data_{mcp_id}.json", 'w') as file:
        json.dump(data, file)