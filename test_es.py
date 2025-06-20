import json
import os
from elasticsearch import Elasticsearch
from sparse_vec import call_sparse_vec_api
import asyncio
from pymongo import MongoClient
import logging


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)


def create_index(es, index_name, body):
    
    if not es.indices.exists(index=index_name):
        try:
            es.indices.create(index=index_name, body=body)
            print(f"Index '{index_name}' created.")
        except Exception as e:
            logging.error(f"Error creating index: {e}")
    else:
        print(f"Index '{index_name}' already exists.")


def get_text_to_embed(json_data):
    name = json_data.get("id").split("/", 1)[-1]
    text_to_embed = json_data.get("about")
    tools = json_data.get("tools")
    for tool in tools:
        description = tool.get("description")
        if description:
            text_to_embed += " - " + description
    text_to_embed = text_to_embed.strip()
    if len(text_to_embed) == 0:
        text_to_embed = name
    return text_to_embed
    

def get_mongodb_data(connection_string):

    try:
        client = MongoClient(connection_string)
    except Exception as e:
        logging.error(f"Error connecting to MongoDB client: {e}")

    # Select database and collection
    db = client["mcp"]
    collection = db["mcp_server"]

    field_ids = ["about", "tools"]
    projection = {field: 1 for field in field_ids}

    try:
        results = collection.find({}, projection)
    except Exception as e:
        logging.error(f"Error getting MongoDB data: {e}")

    all_data = []
    all_text_to_embed = []

    for doc in results:
        mcp_id = doc.get("id", "")
        name = mcp_id.split("/", 1)[-1]
        about = doc.get("about", "")
        tools = doc.get("tools", [])

        data = {"id": mcp_id,
                "about": about,
                "tools": tools
                }

        text_to_embed = about
        for tool in tools:
            description = tool.get("description")
            if description:
                text_to_embed += " - " + description
        text_to_embed = text_to_embed.strip()
        if len(text_to_embed) == 0:
            text_to_embed = name

        assert validate_mongodb_data(data), "Invalid MongoDB document found"
        all_data.append(data)
        all_text_to_embed.append(text_to_embed)

    return all_data, all_text_to_embed


def add_docs_to_index(es, index_name, doc_path):

    all_data, all_text_to_embed = get_mongodb_data(connection_string)

    BATCH_SIZE = 1

    embeddings = asyncio.run(call_sparse_vec_api(all_text_to_embed, BATCH_SIZE))

    assert len(embeddings) == len(all_data), "Failed to get all embeddings"

    for i, data in enumerate(all_data):
        data['sparse_embed'] = embeddings[i][0]
    
        operations_list.append({
            "index": {
                "_index":index_name
            }
        })
        operations_list.append(data)
        
    resp = es.bulk(operations=operations_list)
    assert not resp["errors"], "Error adding documents."
    

def validate_mongodb_data(doc):
    EXPECTED_KEYS = {
        "id": str,
        "about": str,
        "tools": list,
    }
    EXPECTED_TOOL_KEYS = {
        "name": (str, list, type(None)),
        "description": (str, type(None)),
        "endpoint": str,
        "inputs": str,
        "role": str,
        "parameters": dict,
    }

    for key, expected_type in EXPECTED_KEYS.items():
        if key not in doc or not isinstance(doc[key], expected_type):
            logging.error(f"Incorrect MongoDB data format at key: {key}")
            return False

    if "tools" in doc:
        for item in doc["tools"]:
            for tools_key in item:
                if tools_key not in EXPECTED_TOOL_KEYS.keys():
                    logging.error(f"Incorrect MongoDB data format at key: {tools_key}")
                    return False
    
    return True


def validate_doc_format(doc):
    EXPECTED_KEYS = {
        "id": str,
        "about": str,
        "tools": list,
        "sparse_embed": list,
    }
    EXPECTED_TOOL_KEYS = {
        "name": (str, list, type(None)),
        "description": (str, type(None)),
        "endpoint": str,
        "inputs": str,
        "role": str,
        "parameters": dict,
    }
    EXPECTED_SPARSE_KEYS = {
        "index": int,
        "value": float,
    }

    for key, expected_type in EXPECTED_KEYS.items():
        if key not in doc or not isinstance(doc[key], expected_type):
            print(key)
            return False

    if "tools" in doc:
        for item in doc["tools"]:
            for tools_key in item:
                if tools_key not in EXPECTED_TOOL_KEYS.keys():
                    print(f"tools: {tools_key}")
                    return False
    
    if "sparse_embed" in doc:
        for key, expected_type in EXPECTED_SPARSE_KEYS.items():
            if len(doc["sparse_embed"]) == 0:
                return False
            for item in doc["sparse_embed"]:
                if key not in item or not isinstance(item[key], expected_type):
                    return False
    
    return True


def delete_index(es, index_name):
    try:
        resp = es.indices.delete(index=index_name)
        assert resp["acknowledged"]
    except Exception as e:
        logging.error(f"Failed to delete index: {e}")


def test_mcp(es, connection_string, body):

    # create test index
    test_index_name = "test-mcp-0"
    create_index(es, test_index_name, body)
    add_docs_to_index(es, test_index_name, connection_string)

    # test that index can be queried properly
    query_body_0 = {
                    "query": {
                        "nested": {
                        "path": "tools",
                        "query": {
                            "match": {
                            "tools.name": "make_payment"
                            }
                        }
                        }
                    }
                }
    resp0 = es.search(index=test_index_name, body=query_body_0)
    assert resp0["hits"]["total"]["value"] >= 1, "Queried item not found"
    query_body_1 = {
                    "query": {
                        "term": {
                            "id": "24mlight/a-share-mcp-is-just-i-need"
                        }
                    }
                }
    resp1 = es.search(index=test_index_name, body=query_body_1)
    assert resp1["hits"]["total"]["value"] >= 1, "Queried item not found"


    # validate document format
    scroll = "2m"
    page = es.search(index=test_index_name, scroll=scroll, body={"query": {"match_all": {}}})

    sid = page['_scroll_id']
    scroll_size = len(page['hits']['hits'])
    num_docs = 0

    while scroll_size > 0:
        for hit in page['hits']['hits']:
            doc = hit["_source"]
            assert validate_doc_format(doc), "Document with invalid format found"
            num_docs += 1

        page = es.scroll(scroll_id=sid, scroll=scroll)
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])
    print(f"{num_docs} documents validated.")


    # delete test index
    delete_index(es, test_index_name)

    print("All tests passed.")


            
if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Elasticsearch Index Builder")
    parser.add_argument("--index_name", type=str, required=False, help="Name of index")
    parser.add_argument("--run_tests", action="store_true", help="Whether to run tests")
    args = parser.parse_args()

    BODY = {
        'mappings':{
            "properties": {
                "id": {"type": "keyword"},
                "about": {"type": "text"},
                "tools": {
                    "type": "nested",
                    "properties": {
                        "name": { "type": "text" },
                        "description": { "type": "text" }
                    }
                },
                "sparse_embed": {
                    "type": "nested",
                    "properties": {
                        "index": { "type": "integer" },
                        "value": { "type": "float" }
                    }
                }
            }
        },
    }

    ES_HOST = "https://10.12.160.250:9200"  # could also be https://localhost:9200
    ES_USERNAME = "elastic"
    ES_PASSWORD = "dDkBoU_+qPhooa5FOXja"
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USERNAME, ES_PASSWORD),
        verify_certs=False  # Set to False if using self-signed certs in dev
    )
    if not es.ping():
        print("Unable to connect to Elasticsearch.")
        exit()
    else:
        print("Connected to Elasticsearch.")

    connection_string = "mongodb://mongo.web.web:lQ0ecPUy3u871ECbgsMc5IM0ceiYuHyI@web.mongo.nb.com:27017/admin?retryWrites=true&replicaSet=web&readPreference=secondaryPreferred&connectTimeoutMS=10000&authSource=admin"

    if args.run_tests:
        test_mcp(es, connection_string, BODY)

    else:
        index_name = "test-mcp-1"
        if es.indices.exists(index=index_name):
            delete_index(es, index_name)
        
        create_index(es, index_name, BODY)
        add_docs_to_index(es, index_name, connection_string)
