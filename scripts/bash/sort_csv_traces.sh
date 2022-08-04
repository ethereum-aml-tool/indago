#!/bin/bash
FOLDER_TO_SORT="/data/pruned/traces"
BLOB_NAME="traces-"
HEADER_FILE="traces-header.csv"
OUTPUT_FILE="traces-sorted.csv"
CORES=8
COLUMN_TO_SORT="1"
SEC_COLUMN_TO_SORT="2"

cd $FOLDER_TO_SORT
mkdir -p ./sorted

HEADER=$(head -n 1 ${HEADER_FILE})
rm -f ${HEADER_FILE}

# echo "Sorting blobs..."
# # block (8) -> index (2) -> tree (6)
# for X in ${BLOB_NAME}*; do tail -n+2 $X | LC_ALL=C sort -t',' --parallel=${CORES} -k ${COLUMN_TO_SORT},${COLUMN_TO_SORT}n -k ${SEC_COLUMN_TO_SORT},${SEC_COLUMN_TO_SORT}n > ./sorted/sorted-$X; done

# echo "Removing unsorted blobs..."
# for X in ${BLOB_NAME}*; do rm $X; done

echo "Merging sorted blobs..."
LC_ALL=C sort -t',' --parallel=${CORES} -k ${COLUMN_TO_SORT},${COLUMN_TO_SORT}n -k ${SEC_COLUMN_TO_SORT},${SEC_COLUMN_TO_SORT}n -m sorted-${BLOB_NAME}* > $OUTPUT_FILE
sed -i "1i${HEADER}" $OUTPUT_FILE

#echo "Removing sorted blobs..." 
#rm -r sorted/