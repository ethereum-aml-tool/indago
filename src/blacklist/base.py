import pickle
import pandas as pd
from typing import Optional

from utils.loader import DataLoader


class BaseBlacklist:
    """
    Inherit for blacklisting algorithms.
    """

    def __init__(self, data_loader: DataLoader):
        self.data_loader: DataLoader = data_loader

    def make_blacklist(self, start_block: Optional[int] = None, end_block: Optional[int] = None):
        """
        Make a blacklist, should always be overwritten.
        """
        raise NotImplementedError

    def _save(self, x, file):
        with open(file, 'wb') as fp:
            pickle.dump(x, fp, pickle.HIGHEST_PROTOCOL)

    def _load(self, file):
        with open(file, 'rb') as fp:
            return pickle.load(fp)
