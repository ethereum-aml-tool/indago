# Advanced FIFO where tainted transaction and origin of taint is saved.

# performance for full day: 9.4 sec (most transactions skipped)

# Note: replacing python lists with numpy arrays will probably increase the speed of the Fifo_queue.

# TODO add method for requesting a certain value from fifo_queue and returning a list of values.
# This will help move more functionality into the class hopefully cleaning up the code.


from datetime import datetime
from typing import Optional

import psutil
from src.blacklist.base import BaseBlacklist
from utils.loader import DataframeLoader
import pandas as pd
import numpy as np


class AdvancedFIFO(BaseBlacklist):

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
        n_pretainted = len(self.initial_blacklist)
        queue_dictionary = dict()
        for i in range(len(self.initial_blacklist)):
            new_queue = FifoQueue(5)
            new_queue.add(-10**25, self.initial_blacklist.iat[i], -1)
            queue_dictionary[self.initial_blacklist.iat[i]] = new_queue
        new_queue = FifoQueue(5)
        new_queue.add(10**25, "origin", -1)
        queue_dictionary["block_reward"] = new_queue

        skipped_transactions = 0
        processed_transactions = 0
        chunk_counter = 0
        chunk_size = 10000
        start_time = datetime.now()
        for tx_chunk in self.data_loader.yield_traces(chunk_size, start_block, end_block):
            pd.options.mode.chained_assignment = None
            nonzero_traces = tx_chunk[(tx_chunk.value.astype(int) > 0) & (
                tx_chunk.status == 1)].dropna(subset=["to_address", "value"])
            nonzero_traces.reset_index(inplace=True)
            nonzero_traces.drop("index", axis=1, inplace=True)
            from_adresses = nonzero_traces.iloc[:, 3].astype(str)
            to_addresses = nonzero_traces.iloc[:, 4].astype(str)
            transferred_balances = nonzero_traces.iloc[:, 5]

            for i in range(len(nonzero_traces)):
                from_address = from_adresses[i]
                to_address = to_addresses[i]
                left_to_transfer = transferred_balances[i]

                from_queue = queue_dictionary.get(from_address)
                to_queue = queue_dictionary.get(to_address)

                if(to_queue == None):
                    to_queue = FifoQueue(5)
                    queue_dictionary[to_address] = to_queue

                if from_queue != None and from_queue.get_balance() != 0:
                    print(f'from_queue={from_queue}')
                    print(f'balance={from_queue.get_balance()}')
                    print(f'left_to_transfer={left_to_transfer}')
                if (from_queue != None) and (not from_queue.is_empty()) and from_queue.get_balance() >= left_to_transfer:
                    processed_transactions += 1
                    last_value = 0
                    done = False
                    while not done:
                        queue_value, origin_address, origin_block = from_queue.peak()
                        if to_queue.is_full():
                            to_queue.expand()

                        if queue_value < 0:
                            if last_value != 0:
                                to_queue.add(last_value, "clean", -1)
                                left_to_transfer -= abs(last_value)
                                last_value = 0
                                if to_queue.is_full():
                                    to_queue.expand()

                            if -queue_value > left_to_transfer:
                                to_queue.add(-left_to_transfer,
                                             origin_address, origin_block)
                                from_queue.change_first(
                                    queue_value + left_to_transfer)
                                done = True
                            elif -queue_value == left_to_transfer:
                                to_queue.add(
                                    queue_value, origin_address, origin_block)
                                from_queue.pop()
                                done = True
                            else:
                                to_queue.add(
                                    queue_value, origin_address, origin_block)
                                left_to_transfer += queue_value
                                from_queue.pop()
                        else:
                            current_value = last_value + queue_value
                            if current_value > left_to_transfer:
                                to_queue.add(left_to_transfer, "clean", -1)
                                from_queue.change_first(
                                    current_value - left_to_transfer)
                                done = True
                            elif current_value == left_to_transfer:
                                to_queue.add(current_value, "clean", -1)
                                from_queue.pop()
                                done = True
                            else:
                                last_value = current_value
                                from_queue.pop()
                else:
                    skipped_transactions += 1

            # save some stats every x chunks
            chunk_counter += 1
            if chunk_counter % 100 == 0 or chunk_counter == 1:
                rows_processed = "{:,}".format(chunk_counter * chunk_size)
                n_taints = "{:,}".format(len(queue_dictionary) - n_pretainted)
                max_block = nonzero_traces.block_number.max()
                processed_after = (datetime.now() - start_time)
                ram_usage = round(psutil.virtual_memory().used / (1000**3), 2)

                print(
                    f'\nProcessed chunk {chunk_counter}, ~{rows_processed} total transactions.')
                print(f'{n_taints} addresses in queue dict.')
                print(f'{processed_transactions} processed.')
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
                    'n_tainted': int(n_taints.replace(',', '')),
                    'n_skipped': skipped_transactions,
                    'max_block': max_block,
                    'processed_after': processed_after,
                    'ram_usage_gb': ram_usage,
                }
                run_data: pd.DataFrame = pd.DataFrame.from_dict([run_data])
                if chunk_counter == 1:
                    run_data.to_csv(
                        f'{self.save_dir}/fifo-rundata.csv', index=False)
                else:
                    run_data.to_csv(
                        f'{self.save_dir}/fifo-rundata.csv', mode='a', header=False, index=False)

        for key, value in queue_dictionary.items():
            if key != "block_reward":
                taint, untainted = value.sum_taint()
                if taint > 0:
                    print(
                        f'{key}: balance={value.get_balance()} taint={taint} untaint={untainted}')
        # blacklist_dataframe = pd.DataFrame(queue_dictionary.items(), columns = ["address", "queue"])
        # length = len(blacklist_dataframe)
        # np_array =  np.empty([length, 4], dtype=object)
        # for i in range(length):
        #         address = blacklist_dataframe.address[i]
        #         queue = queue_dictionary[address]
        #         bla = [""] * 4
        #         np_array[i][0] = address
        #         np_array[i][1] = queue.queue_values_to_string()
        #         np_array[i][2] = queue.origin_addresses_to_string()
        #         np_array[i][3] = queue.origin_blocks_to_string()
        # blacklist_dataframe = pd.DataFrame(np_array, columns = ["addresses", "balances", "origin_addresses", "origin_blocks"])

        # #blacklist_dataframe.to_csv(r"C:\Users\maxar\OneDrive\Documents\Chalmers\ExJobb\Git\Blacklisting\experimental_notebooks\results\Advanced_FIFO_blacklist.csv")
        # return blacklist_dataframe
        _save_to_csv(queue_dictionary, f'{self.save_dir}/fifo-result.csv')

        def _save_to_csv(self, data_dict, path):
            raise NotImplementedError


class FifoQueue:
    def __init__(self, max_size):
        if max_size == 0:
            return ValueError("Fifo_queue must be atleast of length 1")
        self.balance = 0
        self.max_size = max_size
        self.queue_values = [0] * max_size
        self.origin_addresses = [""] * max_size
        self.origin_blocks = [-1] * max_size
        self.front = 0
        self.first_empty = 0
        self.full = False
        self.empty = True

    def pop(self):
        self.balance -= abs(self.queue_values[self.front])
        self.front += 1
        if self.front == self.max_size:
            self.front = 0
        if self.first_empty == self.front:
            self.empty = True
        self.full = False

    def add(self, new_value, origin_address, origin_block):
        if self.full == True:
            return BufferError("queue is full")
        self.balance += abs(new_value)
        self.queue_values[self.first_empty] = new_value
        self.origin_addresses[self.first_empty] = origin_address
        self.origin_blocks[self.first_empty] = origin_block
        self.first_empty += 1
        if self.first_empty == self.max_size:
            self.first_empty = 0
        if self.first_empty == self.front:
            self.full = True
        self.empty = False

    def is_empty(self):
        return self.empty

    def is_full(self):
        return self.full

    def peak(self):
        return (self.queue_values[self.front], self.origin_addresses[self.front], self.origin_blocks[self.front])

    def peak_value(self):
        return self.queue_values[self.front]

    def peak_origin_address(self):
        return self.origin_addresses[self.front]

    def peak_origin_block(self):
        return self.origin_blocks[self.front]

    def change_first(self, new_value):
        self.balance -= abs(self.queue_values[self.front]) - abs(new_value)
        self.queue_values[self.front] = new_value

    def expand(self):
        new_max_size = self.max_size + 5
        new_queue_values = [0] * new_max_size
        new_origin_addresses = [""] * new_max_size
        new_origin_blocks = [0] * new_max_size
        old_balance = self.balance
        done = False
        i = 0
        if self.empty:
            done = True
        while not done:
            new_queue_values[i] = self.peak_value()
            new_origin_addresses[i] = self.peak_origin_address()
            new_origin_blocks[i] = self.peak_origin_block()
            self.pop()
            i += 1
            if(self.is_empty()):
                self.empty = False
                done = True

        self.front = 0
        self.first_empty = i
        self.queue_values = new_queue_values
        self.origin_addresses = new_origin_addresses
        self.origin_blocks = new_origin_blocks
        self.max_size = new_max_size
        self.balance = old_balance

    def queue_values_to_string(self):
        done = False
        string = ""
        temp_front = self.front
        while not done:
            if(temp_front == self.first_empty):
                done = True
            else:
                string = string + str(self.queue_values[temp_front]) + ", "
                temp_front += 1
                if temp_front == self.max_size:
                    temp_front = 0
        return string

    def origin_addresses_to_string(self):
        done = False
        string = ""
        temp_front = self.front
        while not done:
            if(temp_front == self.first_empty):
                done = True
            else:
                string = string + str(self.origin_addresses[temp_front]) + ", "
                temp_front += 1
                if temp_front == self.max_size:
                    temp_front = 0
        return string

    def origin_blocks_to_string(self):
        done = False
        string = ""
        temp_front = self.front
        while not done:
            if(temp_front == self.first_empty):
                done = True
            else:
                string = string + str(self.origin_blocks[temp_front]) + ", "
                temp_front += 1
                if temp_front == self.max_size:
                    temp_front = 0
        return string

    def queue_values_to_array(self):
        done = False
        array = [0] * self.max_size
        temp_front = self.front
        i = 0
        while not done:
            if(temp_front == self.first_empty):
                done = True
            else:
                array[i] = self.queue_values[temp_front]
                i += 1
                temp_front += 1
                if temp_front == self.max_size:
                    temp_front = 0
        return array

    def sum_taint(self):
        done = False
        tainted = 0
        untainted = 0
        temp_front = self.front
        temp_full = self.is_full()

        while not done:
            if temp_front == self.first_empty and not temp_full:
                done = True
            else:
                temp_full = False
                value = self.queue_values[temp_front]
                if value < 0:
                    tainted += value
                else:
                    untainted += value
                temp_front += 1
                if temp_front == self.max_size:
                    temp_front = 0
        # return ("tainted: " + str(tainted) + ", untainted: " + str(untainted))
        return tainted, untainted

    def get_balance(self):
        return self.balance
