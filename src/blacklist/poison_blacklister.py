# Updated posion strategy
# Every address involved with other blacklisted address becomes permanently blacklisted

# Probably fastest and simplest algorithm. But probably also the least realistic,
# for example you can send blacklisted coins to others and make all of their funds blacklisted.

# Note 1: This algorithm only finds the addresses that should be considered blacklisted. It doesn't record how many coins these contain.

# Note 2: Currently blacklisting is only done when there is a value transfer. I could also concive a version where a simple interaction
# also constitutes a blacklisting. When in rome...

# performance for a full day:     2.5 sec,  78136 addresses blacklisted

import os
from typing import Optional

import psutil
from src.blacklist.base import BaseBlacklist
from utils.loader import DataframeLoader
from utils.utils import dict_to_csv
import pandas as pd
from datetime import datetime


class Poison(BaseBlacklist):

    def __init__(
        self,
        data_loader: DataframeLoader,
        initial_blacklist: pd.Series,
        save_dir: str,
    ):
        super().__init__(data_loader)
        self.initial_blacklist = initial_blacklist
        self.save_dir = save_dir

    def make_blacklist(self, start_block: Optional[int] = None, end_block: Optional[int] = None):
        blacklist = pd.Series(
            [True] * len(self.initial_blacklist), self.initial_blacklist).to_dict()
        n_preflagged = len(blacklist)

        # TODO Add progress bar (tqdm)?
        chunk_counter = 0
        chunk_size = 10000
        start_time = datetime.now()
        for tx_chunk in self.data_loader.yield_traces(chunk_size, start_block, end_block):
            pd.options.mode.chained_assignment = None
            nonzero_traces = tx_chunk[(tx_chunk.value.astype(int) > 0) & (
                tx_chunk.status == 1)].dropna(subset=["to_address", "value"])
            nonzero_traces = nonzero_traces.reset_index()
            nonzero_traces = nonzero_traces.drop("index", axis=1)
            from_addresses = nonzero_traces.iloc[:, 3]
            to_addresses = nonzero_traces.iloc[:, 4]

            for i in range(len(nonzero_traces)):
                from_blacklisted = blacklist.get(from_addresses[i])
                if (from_blacklisted):
                    blacklist[to_addresses[i]] = True

            chunk_counter += 1
            if chunk_counter % 1000 == 0 or chunk_counter == 1:
                rows_processed = "{:,}".format(chunk_counter * chunk_size)
                n_blacklisted = "{:,}".format(len(blacklist) - n_preflagged)
                max_block = nonzero_traces.block_number.max()
                processed_after = (datetime.now() - start_time)
                ram_usage = round(psutil.virtual_memory().used / (1000**3), 2)

                print(
                    f'\nProcessed chunk {chunk_counter}, ~{rows_processed} total transactions.')
                print(f'{n_blacklisted} addresses blacklisted.')
                print(
                    f'Reached block {max_block}.')
                print(f'RAM usage: {ram_usage} GB.')
                print(f'{processed_after} time elapsed.')
                print('Working...')

                # log these stats to csv
                run_data = {
                    'chunk': chunk_counter,
                    'rows_processed': int(rows_processed.replace(',', '')),
                    'n_blacklisted': int(n_blacklisted.replace(',', '')),
                    'max_block': max_block,
                    'processed_after': processed_after,
                    'ram_usage_gb': ram_usage
                }
                run_data: pd.DataFrame = pd.DataFrame.from_dict([run_data])
                if chunk_counter == 1:
                    run_data.to_csv(
                        f'{self.save_dir}/poison-rundata.csv', index=False)
                else:
                    run_data.to_csv(
                        f'{self.save_dir}/poison-rundata.csv', mode='a', header=False, index=False)

        print(f'\nSaving results to {self.save_dir}/poison-result.csv...')
        if not os.path.exists(os.path.dirname(self.save_dir)):
            os.makedirs(os.path.dirname(self.save_dir))
        dict_to_csv(blacklist, ['address', 'flagged'],
                    f'{self.save_dir}/poison-result.csv')

        end_time = datetime.now()
        time_taken = end_time - start_time
        print('\nFinished! Total elapsed time: ', time_taken)
