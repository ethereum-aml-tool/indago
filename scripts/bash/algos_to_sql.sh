#!/bin/bash
SAVE_TO_SQL_LOCATION="scripts/save_to_sql.py"
DATA_LOCATION="./data/blacklist_results"

python $SAVE_TO_SQL_LOCATION ${DATA_LOCATION}/haircut.csv --table-name haircut --wei-to-eth balance --columns address balance taint
python $SAVE_TO_SQL_LOCATION ${DATA_LOCATION}/fifo.csv --table-name fifo --wei-to-eth tainted --columns address tainted untainted
python $SAVE_TO_SQL_LOCATION ${DATA_LOCATION}/poison.csv --table-name poison --columns address blacklisted
python $SAVE_TO_SQL_LOCATION ${DATA_LOCATION}/seniority.csv --table-name seniority --wei-to-eth tainted_balance --columns address tainted_balance
