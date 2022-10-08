

# FIFO
# Perserves the order of recieved coins and their respective taint to use in outputs.

# Simiar to seniority principle it tends to perserve the concentration of taint, if not as much.
# Similar to seniority there is a downside that it might be a bit unfair to people who are unaware of the strategy,
# though it makes a bit more sense than seniority principle in that sense.
# Another major downside is that it is the most complex algorthm here.

# performance for a full day:     7.9 sec (most values skipped)

# TODO add method for requesting a certain value from fifo_queue and returning a list of values.
# This will help move more functionality into the class hopefully cleaning up the code.


import csv
from datetime import datetime
from typing import Optional
from IPython.display import clear_output
import psutil
from tqdm import tqdm
from src.blacklist.base import BaseBlacklist
from utils.loader import DataframeLoader
import pandas as pd


class FIFO(BaseBlacklist):

    def __init__(
        self,
        data_loader: DataframeLoader,
        initial_blacklist: pd.Series,
        save_dir: str,
    ):
        super().__init__(data_loader)
        self.initial_blacklist = initial_blacklist
        self.save_dir = save_dir

    def make_blacklist(self, start_block: Optional[int] = None, end_block: Optional[int] = None) -> pd.DataFrame:
        # tornado = pd.read_csv(r'C:\Users\maxar\OneDrive\Documents\Chalmers\ExJobb\Git\Blacklisting\data\tornado.csv')
        # blacklist_dictionary = dict()
        # for i in range(len(tornado)):
        #     new_queue = FifoQueue(5)
        #     new_queue.add(-10**25)
        #     blacklist_dictionary[tornado.address.iat[i]] = new_queue
        # new_queue = FifoQueue(5)
        # new_queue.add(10**25)
        # blacklist_dictionary["block_reward"] = new_queue
        n_pretainted = len(self.initial_blacklist)
        queue_dictionary = dict()
        for i in range(len(self.initial_blacklist)):
            new_queue = FifoQueue(5)
            new_queue.add(-10**45)
            queue_dictionary[self.initial_blacklist.iat[i]] = new_queue
        new_queue = FifoQueue(5)
        new_queue.add(10**45)
        queue_dictionary["block_reward"] = new_queue

        processed_transactions = 0
        forced_transactions = 0
        Transactions_from_none = 0
        chunk_counter = 0
        chunk_size = 100000
        skipped = 0

        start_time = datetime.now()
        pbar = tqdm(total=end_block -
                    (start_block if start_block is not None else 0))
        for tx_chunk in self.data_loader.yield_traces(chunk_size, start_block, end_block):
            pd.options.mode.chained_assignment = None

            nonzero_traces = tx_chunk[(tx_chunk.value > 0) & (
                tx_chunk.status == 1)].dropna(subset=["to_address", "value"])
            nonzero_traces.reset_index(inplace=True)
            nonzero_traces.drop("index", axis=1, inplace=True)

            from_addresses = nonzero_traces.iloc[:, 3].astype(str)
            to_addresses = nonzero_traces.iloc[:, 4].astype(str)
            transferred_balances = nonzero_traces.iloc[:, 5]

            for i in range(len(nonzero_traces)):
                from_address = from_addresses[i]
                to_address = to_addresses[i]
                left_to_transfer = int(transferred_balances[i])

                if from_address == "nan":
                    from_address = "block_reward"

                from_queue = queue_dictionary.get(from_address)
                to_queue = queue_dictionary.get(to_address)

                if(to_queue == None):
                    to_queue = FifoQueue(5)
                    queue_dictionary[to_address] = to_queue

                if (from_queue != None) and (not from_queue.is_empty()) and from_queue.get_balance() >= left_to_transfer:
                    current_value = 0
                    done = False
                    while not done:
                        queue_value = from_queue.peak()
                        if ((queue_value > 0) != (current_value > 0)) and current_value != 0:
                            to_queue.add(current_value)
                            left_to_transfer -= abs(current_value)
                            current_value = queue_value
                        else:
                            current_value += queue_value

                        if to_queue.is_full():
                            to_queue.expand()

                        if current_value < 0:
                            if -current_value > left_to_transfer:
                                to_queue.add(-left_to_transfer)
                                from_queue.change_first(
                                    current_value + left_to_transfer)
                                done = True
                            elif -current_value == left_to_transfer:
                                to_queue.add(-left_to_transfer)
                                from_queue.pop()
                                done = True
                            else:
                                from_queue.pop()
                        else:
                            if current_value > left_to_transfer:
                                to_queue.add(left_to_transfer)
                                from_queue.change_first(
                                    current_value - left_to_transfer)
                                done = True
                            elif current_value == left_to_transfer:
                                to_queue.add(left_to_transfer)
                                from_queue.pop()
                                done = True
                            else:
                                from_queue.pop()
                else:
                    skipped += 1

            # save some stats every x chunks
            chunk_counter += 1
            if chunk_counter % 10 == 0 or chunk_counter == 1:
                rows_processed = "{:,}".format(chunk_counter * chunk_size)
                n_taints = "{:,}".format(len(queue_dictionary) - n_pretainted)
                max_block = nonzero_traces.block_number.max()
                processed_after = (datetime.now() - start_time)
                ram_usage = round(psutil.virtual_memory().used / (1000**3), 2)

                pbar.update(max_block - pbar.n)

                # print(
                #     f'\nProcessed chunk {chunk_counter}, ~{rows_processed} total transactions.')
                # print(f'{n_taints} addresses in queue dict.')
                # print(f'{processed_transactions} processed.')
                # print(f'Reached block {max_block}.')
                # print(f'{processed_after} time elapsed.')
                # print(f'RAM usage: {ram_usage} GB.')
                # print(
                #     f'{len(nonzero_traces)} valid transactions in current chunk...')
                # #print(f'{skipped_transactions} transactions skipped.')
                # print(f'{forced_transactions} transactions forced.')
                # print(f'{Transactions_from_none} transactions from None.')

                # log these stats to csv
                run_data = {
                    'chunk': chunk_counter,
                    'rows_processed': int(rows_processed.replace(',', '')),
                    'n_tainted': int(n_taints.replace(',', '')),
                    # 'n_skipped': skipped_transactions,
                    'n_none_transactions': Transactions_from_none,
                    'n_forced': forced_transactions,
                    'max_block': max_block,
                    'processed_after': processed_after,
                    'ram_usage_gb': ram_usage,
                }
                run_data: pd.DataFrame = pd.DataFrame.from_dict([run_data])
                if chunk_counter == 1:
                    run_data.to_csv(
                        f'{self.save_dir}/fifo-simple-rundata.csv', index=False)
                else:
                    run_data.to_csv(
                        f'{self.save_dir}/fifo-simple-rundata.csv', mode='a', header=False, index=False)

        blacklist_dataframe = pd.DataFrame(
            queue_dictionary.items(), columns=["address", "queue"])
        blacklist_dictionary_str = dict()
        for i in range(len(blacklist_dataframe)):
            address = blacklist_dataframe.address[i]
            queue = queue_dictionary[address]
            blacklist_dictionary_str[address] = queue.to_string()
        blacklist_dataframe = pd.DataFrame(
            blacklist_dictionary_str.items(), columns=["address", "queue"])

        pbar.update(end_block - pbar.n)
        pbar.close()
        self._save_to_csv(queue_dictionary, f'{self.save_dir}/fifo-simple-result.csv')
        print(f'Finished processing {end_block:n} blocks.')

    def _save_to_csv(self, data_dict, path):
        HEADER = ["address", "untainted", "tainted"]
        try:
            with open(path, 'w') as f:
                writer = csv.writer(f)
                writer.writerow(HEADER)
                for address, queue in data_dict.items():
                    if address != "block_reward":
                        taint, untaint = queue.sum_taint()
                        writer.writerow(
                            [address, untaint, taint])
        except IOError:
            print("I/O error")


class FifoQueue:
    def __init__(self, max_size):
        if max_size == 0:
            return ValueError("Fifo_queue must be atleast of length 1")
        self.balance = 0
        self.max_size = max_size
        self.queue = [0] * max_size
        self.front = 0
        self.first_empty = 0
        self.full = False
        self.empty = True

    def pop(self):
        self.balance -= abs(self.queue[self.front])
        self.front += 1
        if self.front == self.max_size:
            self.front = 0
        if self.first_empty == self.front:
            self.empty = True
        self.full = False

    def add(self, new_value):
        if self.full == True:
            return BufferError("queue is full")
        self.balance += abs(new_value)
        self.queue[self.first_empty] = new_value
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
        return self.queue[self.front]

    def change_first(self, new_value):
        self.balance -= abs(self.queue[self.front]) - abs(new_value)
        self.queue[self.front] = new_value

    def expand(self):
        new_max_size = self.max_size + 5
        new_queue = [0] * new_max_size
        old_balance = self.balance
        done = False
        i = 0
        if self.empty:
            done = True
        while not done:
            new_queue[i] = self.peak()
            self.pop()
            i += 1
            if(self.is_empty()):
                self.empty = False
                done = True
        self.front = 0
        self.first_empty = i
        self.queue = new_queue
        self.max_size = new_max_size
        self.balance = old_balance

    def to_string(self):
        done = False
        string = ""
        temp_front = self.front
        while not done:
            if(temp_front == self.first_empty):
                done = True
            else:
                string = string + str(self.queue[temp_front]) + ", "
                temp_front += 1
                if temp_front == self.max_size:
                    temp_front = 0
        return string

    def to_array(self):
        done = False
        array = [0] * self.max_size
        temp_front = self.front
        i = 0
        while not done:
            if(temp_front == self.first_empty):
                done = True
            else:
                array[i] = self.queue[temp_front]
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
                value = self.queue[temp_front]
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
