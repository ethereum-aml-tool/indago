##!/bin/bash
ALGORITHM="poison"
OUTPUT_DIR="/data/blacklist"
TRACES_CSV="${OUTPUT_DIR}/traces-sorted.csv"
KNOWN_ADDRESSES_CSV="${OUTPUT_DIR}/known-addresses.csv"
TORNADO_CSV="${OUTPUT_DIR}/tornado.csv"
BUCKET_NAME="indago"

mkdir -p $OUTPUT_DIR

echo "Downloading datasets if needed..."
if test -f $KNOWN_ADDRESSES_CSV; then
    echo "known-addresses.csv already exists, skipping download"
else
    gsutil -m cp gs://${BUCKET_NAME}/known-addresses.csv $OUTPUT_DIR
fi
if test -f $TORNADO_CSV; then
    echo "tornado.csv already exists, skipping download"
else
    gsutil -m cp gs://${BUCKET_NAME}/tornado.csv $OUTPUT_DIR
fi
if test -f $TRACES_CSV; then
    echo "traces-sorted.csv already exists, skipping download"
else
    gsutil -m cp gs://${BUCKET_NAME}/sorted/traces-sorted.csv $OUTPUT_DIR
fi

echo "Running algorithm..."
cd /v2
cargo run --release

echo "Uploading result..."
gsutil -m cp -r $OUTPUT_DIR gs://${BUCKET_NAME}/${ALGORITHM}/
