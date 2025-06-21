#!/bin/bash
# run_es.sh
# A script to get MongoDB data and create Elasticsearch index

source ~/es-mcp/bin/activate
cd ~/mcp_elastic_search
python3 test_es.py