#!/bin/bash
FOLDER_TO_SORT="/home/ponbac/dev/indago/data/raw/blocks"
BLOB_NAME="blocks-"
HEADER_FILE="blocks-header.csv"
OUTPUT_FILE="blocks-sorted.csv"
CORES=8
BUCKET_NAME="indago"
COLUMN_TO_SORT="2"
SEC_COLUMN_TO_SORT="1"

cd $FOLDER_TO_SORT
# mkdir -p ./sorted

# HEADER=$(head -n 1 ${HEADER_FILE})
# rm -f ${HEADER_FILE}

# echo "Sorting blobs..."
# # block (8) -> index (2) -> tree (6)
# for X in ${BLOB_NAME}*; do tail -n+2 $X | LC_ALL=C sort -t',' --parallel=${CORES} -k ${COLUMN_TO_SORT},${COLUMN_TO_SORT}n -k ${SEC_COLUMN_TO_SORT},${SEC_COLUMN_TO_SORT}n >./sorted/sorted-$X; done

# echo "Removing unsorted blobs..."
# for X in ${BLOB_NAME}*; do rm $X; done

# echo "Merging sorted blobs..."
# LC_ALL=C sort -t',' --parallel=${CORES} -k ${COLUMN_TO_SORT},${COLUMN_TO_SORT}n -k ${SEC_COLUMN_TO_SORT},${SEC_COLUMN_TO_SORT}n -m ./sorted/sorted-${BLOB_NAME}* >$OUTPUT_FILE
# sed -i "1i${HEADER}" $OUTPUT_FILE

# echo "Removing sorted blobs..."
# rm -r sorted/

echo "Uploading result..."
gsutil -m cp ${OUTPUT_FILE} gs://${BUCKET_NAME}/blocks-sorted.csv