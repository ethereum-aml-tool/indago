#!/bin/bash
BLOCKS_CSV="/data/dar/blocks-sorted.csv"
TRANSACTIONS_CSV="/data/dar/transactions-sorted.csv"
KNOWN_ADDRESSES_CSV="/data/dar/known-addresses.csv"
OUTPUT_DIR="/data/dar"
BUCKET_NAME="eth-aml-data"

mkdir -p $OUTPUT_DIR

echo "Downloading transaction data..."
if test -f $KNOWN_ADDRESSES_CSV; then
    echo "known-addresses.csv already exists, skipping download"
else
    gsutil -m cp gs://${BUCKET_NAME}/known-addresses.csv $OUTPUT_DIR
fi
if test -f $BLOCKS_CSV; then
    echo "blocks-sorted.csv already exists, skipping download"
else
    gsutil -m cp gs://${BUCKET_NAME}/sorted/blocks-sorted.csv $OUTPUT_DIR
fi
if test -f $TRANSACTIONS_CSV; then
    echo "transactions-sorted.csv already exists, skipping download"
else
    gsutil -m cp gs://${BUCKET_NAME}/sorted/transactions-sorted.csv $OUTPUT_DIR
fi

# generate data.csv and metadata.csv
echo "Generating data.csv and metadata.csv..."
python3 scripts/run_deposit.py $BLOCKS_CSV $TRANSACTIONS_CSV $KNOWN_ADDRESSES_CSV $OUTPUT_DIR

# prune data.csv and metadata.csv
echo "Pruning data.csv and metadata.csv..."
DATA_PRUNED_CSV=${OUTPUT_DIR}/data-pruned.csv
METADATA_PRUNED_CSV=${OUTPUT_DIR}/metadata-pruned.csv
python3 scripts/prune_data.py ${OUTPUT_DIR}/data.csv $DATA_PRUNED_CSV
python3 scripts/prune_metadata.py ${OUTPUT_DIR}/metadata.csv $METADATA_PRUNED_CSV

# generate graph
echo "Generating graph..."
# python3 scripts/run_nx.py $DATA_PRUNED_CSV $OUTPUT_DIR
python3 scripts/run_ig.py $DATA_PRUNED_CSV $OUTPUT_DIR

# upload result
echo "Uploading result..."
gsutil -m cp ${OUTPUT_DIR}/transactions.csv gs://${BUCKET_NAME}/dar-new/transactions.csv
gsutil -m cp ${OUTPUT_DIR}/data-pruned.csv gs://${BUCKET_NAME}/dar-new/data-pruned.csv
gsutil -m cp ${OUTPUT_DIR}/metadata-pruned.csv gs://${BUCKET_NAME}/dar-new/metadata-pruned.csv
gsutil -m cp ${OUTPUT_DIR}/user_clusters.json gs://${BUCKET_NAME}/dar-new/user_clusters.json
gsutil -m cp ${OUTPUT_DIR}/exchange_clusters.json gs://${BUCKET_NAME}/dar-new/exchange_clusters.json
gsutil -m cp ${OUTPUT_DIR}/user_clusters_map.json gs://${BUCKET_NAME}/dar-new/user_clusters_map.json
gsutil -m cp ${OUTPUT_DIR}/exchange_clusters_map.json gs://${BUCKET_NAME}/dar-new/exchange_clusters_map.json