

#FIFO 
# Perserves the order of recieved coins and their respective taint to use in outputs. 

# Simiar to seniority principle it tends to perserve the concentration of taint, if not as much. 
# Similar to seniority there is a downside that it might be a bit unfair to people who are unaware of the strategy,
# though it makes a bit more sense than seniority principle in that sense. 
# Another major downside is that it is the most complex algorthm here. 

# performance for a full day:     7.9 sec (most values skipped)

# TODO add method for requesting a certain value from fifo_queue and returning a list of values. 
# This will help move more functionality into the class hopefully cleaning up the code. 


from IPython.display import clear_output
from src.blacklist.base import BaseBlacklist
from utils.loader import DataframeLoader
import pandas as pd

class FIFO(BaseBlacklist):

    def __init__(
        self,
        data_loader: DataframeLoader,
        n_steps: int,
        save_dir: str,
    ):
        super().__init__(data_loader)
        self.save_dir = save_dir

    def make_blacklist(self) -> pd.DataFrame:
        tornado = pd.read_csv(r'C:\Users\maxar\OneDrive\Documents\Chalmers\ExJobb\Git\Blacklisting\data\tornado.csv')
        blacklist_dictionary = dict()
        for i in range(len(tornado)):
            new_queue = FifoQueue(5)
            new_queue.add(-10**25)
            blacklist_dictionary[tornado.address.iat[i]] = new_queue
        new_queue = FifoQueue(5)
        new_queue.add(10**25)
        blacklist_dictionary["block_reward"] = new_queue

        skipped = 0

        for tx_chunk in self.data_loader.yield_transactions():
            nonzero_traces = tx_chunk[tx_chunk.value > 0.0]
            nonzero_traces.reset_index(inplace=True)
            nonzero_traces.drop("index", axis=1, inplace=True)
            from_adresses = nonzero_traces.iloc[:,2]
            to_addresses = nonzero_traces.iloc[:,3]
            transferred_balances = nonzero_traces.iloc[:,4]


            #progress = 0
            for i in range(len(nonzero_traces)):
                if i % 10000 == 0:
                    progress += 1
                    clear_output(wait=True)
                    print(progress/62.7414)
                    
                from_address = from_adresses[i]
                to_address = to_addresses[i]
                left_to_transfer = transferred_balances[i]
                
                from_queue = blacklist_dictionary.get(from_address)
                to_queue = blacklist_dictionary.get(to_address)

                if(to_queue == None):
                    to_queue = FifoQueue(5)
                    blacklist_dictionary[to_address] = to_queue
                
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
                                from_queue.change_first(current_value + left_to_transfer)
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
                                from_queue.change_first(current_value - left_to_transfer)
                                done = True
                            elif current_value == left_to_transfer:
                                to_queue.add(left_to_transfer)
                                from_queue.pop()
                                done = True
                            else:
                                from_queue.pop()   
                else:
                    skipped += 1
            #print("skipped transactions: " + str(skipped))        
        
        blacklist_dataframe = pd.DataFrame(blacklist_dictionary.items(), columns = ["address", "queue"]) 
        blacklist_dictionary_str = dict()
        for i in range(len(blacklist_dataframe)):
            address = blacklist_dataframe.address[i]
            queue = blacklist_dictionary[address]
            blacklist_dictionary_str[address] = queue.to_string()
        blacklist_dataframe = pd.DataFrame(blacklist_dictionary_str.items(), columns = ["address", "queue"]) 

        #blacklist_dataframe.to_csv(r"C:\Users\maxar\OneDrive\Documents\Chalmers\ExJobb\Git\Blacklisting\experimental_notebooks\results\FIFO_blacklist.csv")
        return blacklist_dataframe

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
        return ("tainted: " + str(tainted) + ", untainted: " + str(untainted))
    
    def get_balance(self):
        return self.balance
