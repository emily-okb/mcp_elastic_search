import json
import os
from typing import List, Dict, Any
from termcolor import colored
from elasticsearch import Elasticsearch
from multiprocessing import Pool


def create_index(es, index_name):
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
                }
            }
        },
    }
    
    if not es.indices.exists(index=index_name):
        try:
            es.indices.create(index=index_name, body=BODY)
            print(f"Index '{index_name}' created.")
        except Exception as e:
            print(f"Error: {e}")
    else:
        print(f"Index '{index_name}' already exists.")


def add_docs_to_index(es, index_name, doc_path):
    operations_list = []
    for filename in os.listdir(doc_path):
        fullpath = os.path.join(doc_path, filename)
        if os.path.isfile(fullpath) and filename.lower().endswith(".json"):
            with open(fullpath, 'r') as file:
                data = json.load(file)
            operations_list.append({
                "index": {
                    "_index":index_name
                }
            })
            operations_list.append(data)

    resp = es.bulk(operations=operations_list)
    print(resp)


            
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Elasticsearch Index Builder")
    parser.add_argument("--doc_path", type=str, required=True, help="Path to document with MCP data")
    args = parser.parse_args()


    es = Elasticsearch("http://search.es.nb.com:9200")
    if not es.ping():
        print("Unable to connect to Elasticsearch.")
        exit()
    else:
        print("Connected to Elasticsearch.")
    index_name = "emily-mcp-index"
    
    create_index(es, index_name)
    
    add_docs_to_index(es, index_name, args.doc_path)
