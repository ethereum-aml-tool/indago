# Haircut strategy

# The main problem with this strategy is that it has to know the balance of every address on the entire blockchain for it to be accurate.
# This eats ram like hell. only the eth balance for every address takes 11.84 GB and to also keep track of all the taint and for all the different tokens it would require more.
# On top of this we also have to have memory left over for the transaction data.

# performance for a full day: 6.3 sec, 16832 addresses with tainted coins.

import csv
from operator import countOf
from typing import Optional
import psutil
from src.blacklist.base import BaseBlacklist
from utils.loader import DataframeLoader
from datetime import datetime
import pandas as pd


class Haircut(BaseBlacklist):

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
        balances = pd.Series([10.0**25] * len(self.initial_blacklist),
                             range(len(self.initial_blacklist))).to_dict()
        taints = pd.Series([1.0] * len(self.initial_blacklist),
                           range(len(self.initial_blacklist))).to_dict()
        addresses = pd.Series(
            range(len(self.initial_blacklist)), self.initial_blacklist).to_dict()
        n_pretainted = len(taints)

        skipped_transactions = 0
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
                # TODO Clean: Temporary fix for empty from addresses
                if from_address == "nan":
                    to_address = to_addresses[i]
                    transferred_balance = transferred_balances[i]

                    to_index = addresses.get(to_address)
                    if (to_index == None):
                        to_index = len(addresses)
                        addresses[to_address] = to_index
                        balances[to_index] = 0
                        taints[to_index] = 0

                    old_balance = balances[to_index]
                    new_balance = old_balance + transferred_balance
                    balances[to_index] = new_balance
                    continue

                to_address = to_addresses[i]
                transferred_balance = transferred_balances[i]

                to_index = addresses.get(to_address)
                from_index = addresses.get(from_address)

                if (to_index == None):
                    to_index = len(addresses)
                    addresses[to_address] = to_index
                    balances[to_index] = 0
                    taints[to_index] = 0

                if (from_index == None):
                    from_index = len(addresses)
                    addresses[from_address] = from_index
                    balances[from_index] = 0
                    taints[from_index] = 0

                remaining_balance = balances[from_index] - transferred_balance
                old_balance = balances[to_index]
                new_balance = old_balance + transferred_balance

                # When running without all historical data blances might become negative, which we skip.
                # If this skipp is triggered even when all data is present something is wrong?
                if remaining_balance < 0:
                    balances[from_index] = 0
                    if remaining_balance < -5e17:  # 0.5 ETH
                        skipped_transactions += 1
                    # print(f'Skipped transaction from {from_address} to {to_address} with value {transferred_balance}')
                    # print(f'Remaining balance: {remaining_balance}')
                    # print(f'New balance: {new_balance}')
                else:
                    balances[from_index] = remaining_balance

                balances[to_index] = new_balance
                taints[to_index] = (taints.get(from_index, 0) * transferred_balance / new_balance) + (
                    taints.get(to_index, 0) * old_balance / new_balance)

                # Need to solve indices getting out of sync to do this properly...
                # if taints.get(to_index, -1) == 0:
                #     taints.pop(to_index)
                # if taints.get(from_index, -1) == 0:
                #     taints.pop(from_index)

            # save some stats every x chunks
            chunk_counter += 1
            if chunk_counter % 1000 == 0 or chunk_counter == 1:
                rows_processed = "{:,}".format(chunk_counter * chunk_size)
                n_blacklisted = "{:,}".format(
                    len(taints) - (n_pretainted + countOf(taints.values(), 0)))
                max_block = nonzero_traces.block_number.max()
                processed_after = (datetime.now() - start_time)
                ram_usage = round(psutil.virtual_memory().used / (1000**3), 2)

                print(
                    f'\nProcessed chunk {chunk_counter}, ~{rows_processed} total transactions.')
                print(f'{n_blacklisted} addresses blacklisted.')
                print(f'Reached block {max_block}.')
                print(f'{processed_after} time elapsed.')
                print(f'RAM usage: {ram_usage} GB.')
                print(
                    f'{len(nonzero_traces)} valid transactions in current chunk...')
                print(f'{skipped_transactions} transactions skipped.')

                # log these stats to csv
                run_data = {
                    'chunk': chunk_counter,
                    'rows_processed': int(rows_processed.replace(',', '')),
                    'n_blacklisted': int(n_blacklisted.replace(',', '')),
                    'n_skipped': skipped_transactions,
                    'max_block': max_block,
                    'processed_after': processed_after,
                    'ram_usage_gb': ram_usage,
                }
                run_data: pd.DataFrame = pd.DataFrame.from_dict([run_data])
                if chunk_counter == 1:
                    run_data.to_csv(
                        f'{self.save_dir}/haircut-rundata.csv', index=False)
                else:
                    run_data.to_csv(
                        f'{self.save_dir}/haircut-rundata.csv', mode='a', header=False, index=False)

        print(
            f'\nFinished processing {chunk_counter} chunks, now writing to {self.save_dir}/haircut-result.csv')
        self._save_to_csv(addresses, balances, taints,
                          f'{self.save_dir}/haircut-result.csv')

    def _save_to_csv(self, addresses, balances, taints, path):
        HEADER = ["address", "balance", "taint"]

        try:
            with open(path, 'w') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(HEADER)
                for key in addresses.keys():
                    row = [key, balances[addresses[key]],
                           taints[addresses[key]]]
                    writer.writerow(row)
        except IOError:
            print("I/O error")
