import os
from typing import List, Optional
from urllib import response
import botocore
from tqdm import tqdm
from boto3 import client, resource
from utils.storage.base import EthereumStorage


class S3Storage(EthereumStorage):
    def __init__(self):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
        self.client = client('s3')
        # assert len(os.environ['AWS_CREDENTIALS']) > 0, "Set AWS_CREDENTIALS prior to use."

    # TODO Possible with existing client? No resource?
    def empty_bucket(self, bucket: str):
        s3 = resource('s3')
        bucket = s3.Bucket(bucket)
        bucket.objects.all().delete()

    def exists(self, bucket: str, file: str) -> bool:
        # Assume file is not a folder if it contains a dot
        if '.' in file:
            try:
                response = self.client.head_object(Bucket=bucket, Key=file)
            except botocore.exceptions.ClientError as e:
                # 404 - object does not exist
                if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                    return False
                # 403 - access denied, no s3:ListBucket permission
                elif e.response['ResponseMetadata']['HTTPStatusCode'] == 403:
                    print(f'Access denied to {bucket}/{file}')
                    raise e
                # Should not reach this print
                print(e)

            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        # If folder, check if folder exists
        path = file.rstrip('/')
        resp = self.client.list_objects(
            Bucket=bucket, Prefix=path, Delimiter='/', MaxKeys=1)
        return 'CommonPrefixes' in resp

    # TODO The -1 default on max_blobs should not be needed?
    def download(self, bucket: str, dir_path: str, out_dir: str, max_blobs: Optional[int] = None):
        if '.' in dir_path:
            dest_pathname = os.path.join(out_dir, dir_path.split('/')[-1])
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            self.client.download_file(bucket, dir_path, dest_pathname)
        else:
            keys = []
            dirs = []
            next_token = ''
            base_kwargs = {
                'Bucket': bucket,
                'Prefix': dir_path,
            }
            while next_token is not None:
                kwargs = base_kwargs.copy()
                if next_token != '':
                    kwargs.update({'ContinuationToken': next_token})
                results = self.client.list_objects_v2(**kwargs)
                contents = results.get('Contents')
                for i in contents:
                    k = i.get('Key')
                    if k[-1] != '/':
                        keys.append(k)
                    else:
                        dirs.append(k)
                next_token = results.get('NextContinuationToken')
            for d in dirs:
                dest_pathname = os.path.join(out_dir, d)
                if not os.path.exists(os.path.dirname(dest_pathname)):
                    os.makedirs(os.path.dirname(dest_pathname))
            for i, k in enumerate(keys):
                if max_blobs is not None and i >= max_blobs:
                    break
                dest_pathname = os.path.join(out_dir, k)
                if not os.path.exists(os.path.dirname(dest_pathname)):
                    os.makedirs(os.path.dirname(dest_pathname))
                self.client.download_file(bucket, k, dest_pathname)


if __name__ == '__main__':
    #storage: EthereumStorage = S3Storage()
    #print(storage.exists('indago-crypto-aml-exjobb', 'traces/'))
    #storage.download('indago-crypto-aml-exjobb', 'traces/', './data/aws/')
    pass
