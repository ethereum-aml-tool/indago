from typing import List, Optional, Protocol


class EthereumStorage(Protocol):
    '''
    Inherit for storage solutions.
    '''

    def empty_bucket(self, bucket: str) -> None:
        '''
        Remove all content from the bucket.
        '''
        raise NotImplementedError

    def exists(self, bucket: str, file: str) -> bool:
        '''
        Check if file or folder exists within the bucket.
        '''
        raise NotImplementedError

    def download(self, bucket: str, dir_path: str, out_dir: str, max_blobs: Optional[int], use_cols: Optional[List[str]]) -> None:
        '''
        Download all files from dir_path in bucket and save them to the out_dir.
        
        {max_blobs} sets a limit on the number of files to download.
        '''
        raise NotImplementedError

    def upload(self, blob_name: str, path_to_file: str, bucket: str) -> str:
        '''
        Upload data to a bucket.
        '''
        raise NotImplementedError