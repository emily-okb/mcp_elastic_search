import json
import os
from elasticsearch import Elasticsearch


def create_index(es, index_name, body):
    
    if not es.indices.exists(index=index_name):
        try:
            es.indices.create(index=index_name, body=body)
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
    assert not resp["errors"], "Error adding documents."


def validate_doc_format(doc):
    EXPECTED_KEYS = {
        "id": str,
        "about": str,
        "tools": list
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
            print(doc[key])
            return False

    if "tools" in doc:
        for item in doc["tools"]:
            for tools_key in item:
                if tools_key not in EXPECTED_TOOL_KEYS.keys():
                    return False
    return True


def test_mcp(es, doc_path, body):

    # create test index
    test_index_name = "test_mcp_0"
    create_index(es, test_index_name, body)
    add_docs_to_index(es, test_index_name, doc_path)

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
    resp = es.indices.delete(index=test_index_name)
    assert resp["acknowledged"], f"Error deleting index"


    print("All tests passed.")


            
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Elasticsearch Index Builder")
    parser.add_argument("--index_name", type=str, required=False, help="Name of index")
    parser.add_argument("--doc_path", type=str, required=False, help="Path to document with MCP data")
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
                }
            }
        },
    }

    es = Elasticsearch("http://search.es.nb.com:9200")
    if not es.ping():
        print("Unable to connect to Elasticsearch.")
        exit()
    else:
        print("Connected to Elasticsearch.")

    doc_path = "mcp_data"
    if args.doc_path:
        doc_path = args.doc_path

    if args.run_tests:
        test_mcp(es, doc_path, BODY)
    else:
        index_name = "emily-mcp-index"
        if args.index_name:
            index_name = args.index_name
        
        create_index(es, index_name, BODY)
        add_docs_to_index(es, index_name, doc_path)
