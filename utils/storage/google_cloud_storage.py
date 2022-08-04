import os
from typing import List, Optional, Dict
import pandas as pd
from google.cloud.storage import Client
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket
from tqdm import tqdm

from utils.storage.base import EthereumStorage


class GoogleCloudStorage(EthereumStorage):
    def __init__(self):
        self.client: Client = Client()
        assert len(os.environ['GOOGLE_APPLICATION_CREDENTIALS']) > 0, \
            "Set GOOGLE_APPLICATION_CREDENTIALS prior to use."

    def empty_bucket(self, bucket: str) -> None:
        bucket: Bucket = self.client.get_bucket(bucket)
        blobs: List[Blob] = bucket.list_blobs()
        for blob in blobs:
            blob.delete()

    def list_blobs(self, bucket: str, prefix: str) -> List[str]:
        bucket: Bucket = self.client.get_bucket(bucket)
        blobs: List[Blob] = bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]

    def exists(self, bucket: str, file: str) -> bool:
        bucket: Bucket = self.client.get_bucket(bucket)
        return bucket.blob(file).exists()

    # TODO Parallelize this.
    def download(self, bucket: str, dir_path: str, out_dir: str, max_blobs: Optional[int] = None, use_cols: Optional[List[str]] = None, dtypes: Optional[Dict] = None, start_offset: Optional[int] = None, end_offset: Optional[int] = None) -> None:
        bucket: Bucket = self.client.get_bucket(bucket)
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir)

        blobs: List[Blob] = list(bucket.list_blobs(prefix=dir_path))
        if start_offset is not None and end_offset is not None:
            blobs = blobs[start_offset:end_offset]
        elif start_offset is not None:
            blobs = blobs[start_offset:]
        elif end_offset is not None:
            blobs = blobs[:end_offset]

        for i in tqdm(range(len(blobs) if max_blobs is None else min(max_blobs, len(blobs)))):
            blob: Blob = blobs[i]
            blob.download_to_filename(os.path.join(out_dir, blob.name.split('/')[-1]))
            if use_cols is not None:
                pd.read_csv(os.path.join(out_dir, blob.name.split('/')[-1]), usecols=use_cols, dtype=dtypes)[use_cols].to_csv(os.path.join(out_dir, blob.name.split('/')[-1]), index=False)

    def upload(self, blob_name: str, path_to_file: str, bucket: str) -> str:
        bucket: Bucket = self.client.get_bucket(bucket)
        blob: Blob = bucket.blob(blob_name)
        blob.upload_from_filename(path_to_file)
        
        return blob.public_url

if __name__ == '__main__':
    storage: EthereumStorage = GoogleCloudStorage()
    print(storage.list_blobs('eth-aml-data', 'sorted/'))
    pass
