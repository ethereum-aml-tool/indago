#!/bin/bash
FOLDER_TO_SORT="/data/transactions"
BLOB_NAME="transactions-"
HEADER_FILE="transactions-000000000001.csv"
OUTPUT_FILE="transactions-sorted.csv"
CORES=8
COLUMN_TO_SORT="6"

cd $FOLDER_TO_SORT
mkdir -p ./sorted

HEADER=$(head -n 1 pruned-${HEADER_FILE})

echo "Removing raw blobs..."
for X in ${BLOB_NAME}*; do rm $X; done

# t=16
echo "Sorting blobs..."
for X in pruned-${BLOB_NAME}*; do tail -n+2 $X | LC_ALL=C sort -t',' --parallel=${CORES} -k ${COLUMN_TO_SORT},${COLUMN_TO_SORT} -n > ./sorted/sorted-$X; done

echo "Removing unsorted blobs..."
for X in pruned-${BLOB_NAME}*; do rm $X; done

echo "Merging sorted blobs..."
LC_ALL=C sort -t',' --parallel=${CORES} -k ${COLUMN_TO_SORT},${COLUMN_TO_SORT} -n -m ./sorted/sorted-pruned-${BLOB_NAME}* > $OUTPUT_FILE
sed -i "1i${HEADER}" $OUTPUT_FILE

echo "Removing sorted blobs..." 
rm -r sorted/