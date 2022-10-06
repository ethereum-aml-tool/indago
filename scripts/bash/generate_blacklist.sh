##!/bin/bash
ALGORITHM="fifo"
OUTPUT_DIR="/data/blacklist"
#OUTPUT_DIR="/media/ponbac/BigHDD/ethereum"
BLOCKS_CSV="${OUTPUT_DIR}/blocks-sorted.csv"
TRACES_CSV="${OUTPUT_DIR}/traces-sorted.csv"
KNOWN_ADDRESSES_CSV="${OUTPUT_DIR}/known-addresses.csv"
TORNADO_CSV="${OUTPUT_DIR}/tornado.csv"
BUCKET_NAME="eth-aml-data"

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
if test -f $BLOCKS_CSV; then
    echo "blocks-sorted.csv already exists, skipping download"
else
    gsutil -m cp gs://${BUCKET_NAME}/sorted/blocks-sorted.csv $OUTPUT_DIR
fi
if test -f $TRACES_CSV; then
    echo "traces-sorted.csv already exists, skipping download"
else
    gsutil -m cp gs://${BUCKET_NAME}/sorted/traces-sorted.csv $OUTPUT_DIR
fi

echo "Running algorithm..."
python3 scripts/run_blacklisting.py $ALGORITHM $BLOCKS_CSV $TRACES_CSV \
 $KNOWN_ADDRESSES_CSV $TORNADO_CSV $OUTPUT_DIR \
  --only-flagged --end-block 14700000

echo "Uploading result..."
gsutil -m cp ${OUTPUT_DIR}/${ALGORITHM}-result.csv gs://${BUCKET_NAME}/blacklisting/${ALGORITHM}-flagged-result.csv
gsutil -m cp ${OUTPUT_DIR}/${ALGORITHM}-rundata.csv gs://${BUCKET_NAME}/blacklisting/${ALGORITHM}-flagged-rundata.csv

echo "Done, removing algorithm files."
rm -f ${OUTPUT_DIR}/${ALGORITHM}-result.csv
rm -f ${OUTPUT_DIR}/${ALGORITHM}-rundata.csv