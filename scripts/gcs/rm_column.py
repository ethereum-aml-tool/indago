'''
Remove the given column from all csv files in the given directory.
'''
import os
import pandas as pd
from tqdm import tqdm


def remove_column(directory: str, column_to_remove: str):
    header = ['block_number', 'transaction_index', 'trace_address', 'from_address',
              'to_address', 'value', 'block_timestamp', 'status']
    for file in tqdm(os.listdir(directory)):
        pd.read_csv(f'{directory}/{file}', names=header).drop(
            columns=[column_to_remove]).to_csv(f'{directory}/{file}', index=False, header=None)


if __name__ == '__main__':
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        description='Remove the given column from all csv files in the given directory.')
    parser.add_argument('directory', type=str,
                        help='The directory to process.')
    parser.add_argument('column_to_remove', type=str,
                        help='The column to remove.')
    args = parser.parse_args()

    remove_column(args.directory, args.column_to_remove)
    print('Done.')
