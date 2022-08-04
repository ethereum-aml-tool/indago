import json
import pickle
from typing import Any, Dict, List
import jsonlines
import csv
import pandas as pd
import numpy as np

from enum import Enum


class Entity(Enum):
    EOA = 0
    DEPOSIT = 1
    EXCHANGE = 2
    DEX = 3
    DEFI = 4
    ICO_WALLETS = 5
    MINING = 6
    TORNADO = 7


class Heuristic(Enum):
    DEPO_REUSE = 0
    SAME_ADDR = 1
    GAS_PRICE = 2
    SAME_NUM_TX = 3
    LINKED_TX = 4
    TORN_MINE = 5
    DIFF2VEC = 6


class JSONSetEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def to_json(obj, path, line_delimited=False, with_pandas=False):
    if with_pandas:
        df = pd.DataFrame(obj)
        df.to_json(path, orient='records', lines=False)
        del df
    elif line_delimited:
        with open(path, 'w') as fp:
            fp.write('[' + '\n')
            for row in obj:
                fp.write(json.dumps(row, cls=JSONSetEncoder) + '\n')
            fp.write('\n' + ']')
    else:
        with open(path, 'w') as fp:
            json.dump(obj, fp, cls=JSONSetEncoder)


def from_json(path):
    with open(path, 'r') as fp:
        return json.load(fp)


def from_jsonlines(path):
    with jsonlines.open(path) as reader:
        return [row for row in reader]


def to_pickle(obj, path):
    with open(path, 'wb') as fp:
        pickle.dump(obj, fp)


def from_pickle(path):
    with open(path, 'rb') as fp:
        return pickle.load(fp)


def to_jsonlines(obj_list, path):
    with jsonlines.open(path, mode='w') as writer:
        writer.write_all(obj_list)


def from_jsonlines(path):
    with jsonlines.open(path) as reader:
        for row in reader:
            yield row


def dict_to_csv(dict: Dict, csv_columns: List[str], path: str):
    try:
        with open(path, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(csv_columns)
            for key in dict.keys():
                writer.writerow([key, dict[key]])
    except IOError:
        print("I/O error")
