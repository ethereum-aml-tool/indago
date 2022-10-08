##!/bin/bash
ALGORITHMS=( "poison" "haircut" "seniority" "tornado_poison" "tornado_haircut" "tornado_seniority"  )
TITLES=("Poison" "Haircut" "Seniority" "Poison - Tornado" "Haircut - Tornado" "Seniority - Tornado" )

RESULT_PATH="/data/results"
RESULT_FILE_NAMES=( "poison-flagged-result.csv" "haircut-flagged-result.csv" "seniority-flagged-result.csv" "poison-tornado-result.csv" "haircut-tornado-result.csv" "seniority-tornado-result.csv" )

OUTPUT_DIRS=("/data/graphs/poison" "/data/graphs/haircut" "/data/graphs/seniority" "/data/graphs/poison_tornado" "/data/graphs/haircut_tornado" "/data/graphs/seniority_tornado")
BUCKET_NAME="eth-aml-data"

mkdir -p $RESULT_PATH

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