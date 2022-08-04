# Seniority principle.
# Always outputs the tainted coins first.

# Big upside is that it generally tends to concentrate the taint which leads to fewer blacklisted addresses.
# The big downside is that it might be a bit unfair to people who are unaware of the strategy.
# Who are we to decide that tainted coins first? Why not last or a completely other strategy?
# In this regard it is easier to justify haircut since it is more "fair" to everyone involved.

# performance for 1/10 of a day:  28 sec         38 addresses balcklisted
# performance for a full day:     4 min 59 sec,  48213 addresses blacklisted


import csv
from datetime import datetime
from typing import Optional

import psutil
from src.blacklist.base import BaseBlacklist
from utils.loader import DataframeLoader
import pandas as pd

from utils.utils import dict_to_csv


class Seniority(BaseBlacklist):

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
            [10.0**25] * len(self.initial_blacklist), self.initial_blacklist).to_dict()
        n_preblacklisted = len(blacklist)

        chunk_counter = 0
        chunk_size = 10000
        start_time = datetime.now()
        for tx_chunk in self.data_loader.yield_traces(chunk_size, start_block, end_block):
            pd.options.mode.chained_assignment = None
            nonzero_traces = tx_chunk[(tx_chunk.value.astype(int) > 0) & (
                tx_chunk.status == 1)].dropna(subset=["to_address", "value"])
            nonzero_traces.reset_index(inplace=True)
            nonzero_traces.drop("index", axis=1, inplace=True)
            from_addresses = nonzero_traces.iloc[:, 3].astype(str)
            to_addresses = nonzero_traces.iloc[:, 4].astype(str)
            transferred_balances = nonzero_traces.iloc[:, 5]

            for i in range(len(nonzero_traces)):
                from_address = from_addresses[i]
                if from_address == "nan":
                    continue
                to_address = to_addresses[i]
                transferred_balance = transferred_balances[i]

                # if transferred_balance == 0:
                #    continue

                from_balance = blacklist.get(from_address)
                if (from_balance != None):
                    remaining_balance = from_balance - transferred_balance
                    if remaining_balance <= 0:
                        transferred_balance -= remaining_balance
                        blacklist.pop(from_address)
                    else:
                        blacklist[from_address] = remaining_balance

                    to_balance = blacklist.get(to_address)
                    if to_balance == None:
                        # if the to_address isn't blacklisted yet it is added here
                        blacklist[to_address] = transferred_balance
                    else:
                        # otherwise the existing value is updated
                        blacklist[to_address] = to_balance + \
                            transferred_balance

            # save some stats every x chunks
            chunk_counter += 1
            if chunk_counter % 1000 == 0 or chunk_counter == 1:
                rows_processed = "{:,}".format(chunk_counter * chunk_size)
                n_blacklisted = "{:,}".format(
                    len(blacklist) - n_preblacklisted)
                max_block = nonzero_traces.block_number.max()
                processed_after = (datetime.now() - start_time)
                ram_usage = round(psutil.virtual_memory().used / (1000**3), 2)

                print(
                    f'\nProcessed chunk {chunk_counter}, ~{rows_processed} total transactions.')
                print(f'{n_blacklisted} addresses blacklisted.')
                print(
                    f'Reached block {max_block}.')
                print(f'{processed_after} time elapsed.')
                print(f'RAM usage: {ram_usage} GB.')
                print(
                    f'{len(nonzero_traces)} valid transactions in current chunk...')

                # log these stats to csv
                run_data = {
                    'chunk': chunk_counter,
                    'rows_processed': int(rows_processed.replace(',', '')),
                    'n_blacklisted': int(n_blacklisted.replace(',', '')),
                    'max_block': max_block,
                    'processed_after': processed_after,
                    'ram_usage_gb': ram_usage,
                }
                run_data: pd.DataFrame = pd.DataFrame.from_dict([run_data])
                if chunk_counter == 1:
                    run_data.to_csv(
                        f'{self.save_dir}/seniority-rundata.csv', index=False)
                else:
                    run_data.to_csv(
                        f'{self.save_dir}/seniority-rundata.csv', mode='a', header=False, index=False)

        print(
            f'\nFinished processing {chunk_counter} chunks, now writing to {self.save_dir}/seniority-result.csv')
        dict_to_csv(blacklist, ['address', 'taint'],
                    f'{self.save_dir}/seniority-result.csv')
