#!/bin/bash

python3 ./scripts/save_to_mongo.py ./data/blacklist_results/haircut.csv --table-name haircut --wei-to-eth balance --columns address balance taint
python3 ./scripts/save_to_mongo.py ./data/blacklist_results/fifo.csv --table-name fifo --wei-to-eth tainted --columns address tainted untainted
python3 ./scripts/save_to_mongo.py ./data/blacklist_results/poison.csv --table-name poison --columns address blacklisted
python3 ./scripts/save_to_mongo.py ./data/blacklist_results/seniority.csv --table-name seniority --wei-to-eth tainted_balance --columns address tainted_balance