##!/bin/bash
ALGORITHMS=( "poison" "haircut" "seniority" "tornado_poison" "tornado_haircut" "tornado_seniority"  )
TITLES=("Poison" "Haircut" "Seniority" "Poison - Tornado" "Haircut - Tornado" "Seniority - Tornado" )

RESULT_PATH="/data/results"
RESULT_FILE_NAMES=( "poison-flagged-result.csv" "haircut-flagged-result.csv" "seniority-flagged-result.csv" "poison-tornado-result.csv" "haircut-tornado-result.csv" "seniority-tornado-result.csv" )

OUTPUT_DIRS=("/data/graphs/poison" "/data/graphs/haircut" "/data/graphs/seniority" "/data/graphs/poison_tornado" "/data/graphs/haircut_tornado" "/data/graphs/seniority_tornado")
BUCKET_NAME="eth-aml-data"

mkdir -p $OUTPUT_DIR

echo "Downloading data if needed..."
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

for i in "${!TITLES[@]}"; do
    echo "Downloading results file if needed..."
    if test -f $RESULT_PATH/${RESULT_FILE_NAMES[$i]}; then
        echo "${RESULT_FILE_NAMES[$i]} already exists, skipping download"
    else
        gsutil -m cp gs://${BUCKET_NAME}/blacklisting/${RESULT_FILE_NAMES[$i]} $RESULT_PATH
    fi

    echo "Generating graph for ${TITLES[$i]}..."
    python3 scripts/analysis/generate_cluster_data.py "${TITLES[$i]}" "${RESULT_PATH}/${RESULT_FILE_NAMES[$i]}" "${OUTPUT_DIRS[$i]}"

    echo "Uploading graph for ${TITLES[$i]}..."
    gsutil -m cp -r "${OUTPUT_DIRS[$i]}" gs://${BUCKET_NAME}/graphs/${ALGORITHMS[$i]}
done