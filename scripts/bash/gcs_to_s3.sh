#!/bin/bash

# gsutil needs to be installed (https://cloud.google.com/storage/docs/gsutil_install#alt-install)
# set up google credentials with gcloud init
# aws credentials in ~/.boto
# https://stackoverflow.com/questions/39329580/exporting-data-from-google-cloud-storage-to-amazon-s3
GOOGLE_CLOUD_BUCKET="eth-aml-data"
S3_BUCKET="indago-crypto-aml-exjobb"

# time gsutil -D -m -o s3:host=s3.eu-north-1.amazonaws.com rsync -r gs://eth-aml-data/test s3://indago-crypto-aml-exjobb/test
time gsutil -m rsync -r gs://${GOOGLE_CLOUD_BUCKET}/balances s3://${S3_BUCKET}/balances