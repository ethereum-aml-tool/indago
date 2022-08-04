from optparse import Option
import os
from typing import Any, Iterable, Dict, Optional, Set
import pandas as pd


class DataLoader:
    """
    Inherit me for new data loaders.
    """

    def get_blocks(self) -> Iterable[Any]:
        raise NotImplementedError

    def yield_transactions(self, chunk_size: int = 10000, start_block: Optional[int] = None,
                           end_block: Optional[int] = None) -> Iterable[Any]:
        raise NotImplementedError

    def yield_traces(self, chunk_size: int = 10000, start_block: Optional[int] = None,
                     end_block: Optional[int] = None) -> Iterable[Any]:
        raise NotImplementedError


class DataframeLoader(DataLoader):
    """
    Standard loader reading CSV files.

    Supports toy Bionic token data from etherclust:
    https://github.com/etherclust/etherclust/tree/master/data

    Supports processed BigQuery database.
    """

    def __init__(
        self,
        block_csv: str,
        known_addresses_csv: str,
        cache_dir: str,
        transaction_csv: Optional[str] = None,
        traces_csv: Optional[str] = None,
        tornado_csv: Optional[str] = None,
    ):
        print('fetching exchanges...')
        known_addresses: pd.DataFrame = pd.read_csv(known_addresses_csv)
        exchanges: pd.DataFrame = known_addresses[
            (known_addresses.account_type == 'eoa') &
            (known_addresses.entity == 'exchange')
        ]
        print(f'found {len(exchanges)} exchanges.')

        # inverse of exchanges (everything else in known addresses should
        # not be considered an EOA or deposit.)
        blacklist: pd.DataFrame = known_addresses[
            (known_addresses.account_type != 'eoa') |
            (known_addresses.entity != 'exchange')
        ]
        print(f'found {len(blacklist)} addresses to ignore.')

        # addresses marked as illegitimate in known_addresses.csv
        flagged: pd.DataFrame = known_addresses[known_addresses.legitimacy == 0]
        print(f'found {len(flagged)} addresses flagged as illegitimate.')

        # find miners
        print('fetching miners...')
        miners_file: str = os.path.join(cache_dir, 'miners.csv')
        miners: pd.Series = self._find_miners(
            block_csv, cache_file=miners_file)

        self._exchanges: pd.DataFrame = exchanges
        self._miners: pd.Series = miners
        self._blacklist: pd.DataFrame = blacklist
        self._flagged: pd.DataFrame = flagged

        self._block_csv = block_csv
        self._transaction_csv = transaction_csv
        self._known_addresses_csv = known_addresses_csv
        self._traces_csv = traces_csv
        self._tornado_csv = tornado_csv

    def get_exchanges(self) -> pd.DataFrame:
        return self._exchanges

    def get_exchange_metadata(self, address: str) -> Dict[str, Any]:
        df: pd.DataFrame = self._exchanges[self._exchanges.address == address]
        if len(df) == 0:
            return {}  # nothing to do
        metadata: Dict[str, Any] = df.iloc[0].to_dict()
        del metadata['address']  # don't need this
        return metadata

    def get_miners(self) -> pd.DataFrame:
        return self._miners

    def get_known_addresses(self) -> pd.DataFrame:
        return pd.read_csv(self._known_addresses_csv)

    def get_blacklist(self) -> pd.DataFrame:
        return self._blacklist

    def get_flagged(self) -> pd.DataFrame:
        return self._flagged

    def get_tornado(self) -> Optional[pd.DataFrame]:
        if self._tornado_csv is None:
            return None
        return pd.read_csv(self._tornado_csv)

    def _find_miners(
        self,
        block_csv: str,
        chunk_size: int = 10000,
        cache_file: Optional[str] = None,
    ) -> pd.Series:
        """
        Load a segment of the block csv at a time and store unique
        miner addresses. Otherwise, it is too large.
        """
        if os.path.isfile(cache_file):
            miners = pd.read_csv(cache_file)
        else:
            miners: Set = set()
            for chunk in pd.read_csv(block_csv, chunksize=chunk_size):
                chunk: pd.DataFrame = chunk
                chunk_miners: pd.DataFrame = chunk.miner.drop_duplicates()
                chunk_miners: Set = set(chunk_miners)
                miners: Set = miners.union(chunk_miners)
            miners: pd.Series = pd.Series(list(miners))

            cache_dir: str = os.path.dirname(cache_file)
            if not os.path.isdir(cache_dir):
                os.makedirs(cache_dir)
            miners.to_csv(cache_file)
        print(f'found {len(miners)} miners.')
        return miners

    def yield_transactions(
        self,
        chunk_size: int = 10000,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None
    ) -> Iterable[pd.DataFrame]:
        """
        Load a segment at a time (otherwise too large).
        """
        if self._transaction_csv is None:
            print('ERROR: no transactions file specified')
            return

        for chunk in pd.read_csv(self._transaction_csv, chunksize=chunk_size):
            if start_block is not None:
                last_block: int = chunk.block_number.iloc[-1]
                if last_block < start_block:
                    continue
            if end_block is not None:
                first_block: int = chunk.block_number.iloc[0]
                if first_block > end_block:
                    break
            yield chunk

    def yield_traces(
        self,
        chunk_size: int = 10000,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None
    ) -> Iterable[pd.DataFrame]:
        """
        Load a segment at a time (otherwise too large).
        """
        if self._traces_csv is None:
            print('ERROR: no traces file specified')
            return

        if start_block is not None:
            print(f'\nStart block enabled, fast-forwarding to block [{start_block}]...')

        for chunk in pd.read_csv(self._traces_csv, chunksize=chunk_size):
            if start_block is not None:
                last_block: int = chunk.block_number.iloc[-1]
                if last_block < start_block:
                    continue
            if end_block is not None:
                first_block: int = chunk.block_number.iloc[0]
                if first_block > end_block:
                    print(f'\nReached end block [{end_block}], quitting.')
                    break
            yield chunk
