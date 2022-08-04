import os
import sys
import csv
import copy
import heapq
import tempfile
from xmlrpc.client import Boolean
import pandas as pd
from glob import glob
from tqdm import tqdm
from typing import Any, List, Optional, final

csv.field_size_limit(2**30)


def main(args: Any):
    filenames = glob(os.path.join(args.csv_dir, '*.csv'))
    if not args.merge_only:
        print('running simple sort...')
        sort_dir: str = os.path.join(args.csv_dir, 'sorted')
        if not os.path.isdir(sort_dir):
            os.makedirs(sort_dir)
        # sort each filename independently
        pbar = tqdm(total=len(filenames))
        for filename in filenames:
            basename: str = os.path.basename(filename)
            outname: str = os.path.join(sort_dir, basename)
            memorysort(filename, outname, sort_order=args.sort_columns)
            pbar.update()
        pbar.close()

    if args.sort_only:
        sys.exit(0)

    # paths to sorted files
    if not args.merge_only:
        filenames = glob(os.path.join(args.csv_dir + '/sorted', '*.csv'))

    # get header
    header: List[str] = get_header(filenames[0])
    merge_idxs = list(map(lambda x: header.index(x), args.sort_columns))

    # merge sorted files together slowly
    print('running merge sort...')
    temp_filename: str = mergesort(filenames, nway=2, merge_idxs=merge_idxs)

    processed_dir: str = os.path.join(args.csv_dir, 'processed')
    if not os.path.isdir(processed_dir):
        os.makedirs(processed_dir)
    out_filename: str = os.path.join(processed_dir, args.out_filename)

    count: int = 0
    with open(out_filename, 'w', newline='') as fp:
        writer: csv.writer = csv.writer(
            fp, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)

        with open(temp_filename, newline='') as sfp:
            for row in csv.reader(sfp):
                writer.writerow(row)

                if count % 1000000 == 0 and count != 0:
                    print(f'Written {count} rows.')

                count += 1

    print('removing temp file...')
    os.remove(temp_filename)


def memorysort(
    filename: str,
    outname: str,
    sort_order=['block_number', 'transaction_index'],  # 'trace_address'
    nan_position="first"
):
    """Sort this CSV file in memory on the given columns"""
    df: pd.DataFrame = pd.read_csv(
        filename, low_memory=False)  # TODO Remove low_memory=False and give dtypes?
    df: pd.DataFrame = df.sort_values(
        by=sort_order, ascending=True, na_position=nan_position)
    df.reset_index(inplace=True)
    df.drop("index", axis=1, inplace=True)
    df.to_csv(outname, index=False)  # overwriting
    del df  # wipe from memory


def mergesort(
    sorted_filenames,
    nway=2,
    merge_idxs=[5, 18]
):
    """Merge two sorted CSV files into a single output."""

    orig_filenames: List[str] = copy.deepcopy(sorted_filenames)
    merge_n: int = 0

    pbar = tqdm(total=len(orig_filenames))
    while len(sorted_filenames) > 1:
        # merge_filenames = current files to sort
        # sorted_filenames = remaining files to sort
        merge_filenames, sorted_filenames = \
            sorted_filenames[:nway], sorted_filenames[nway:]

        with tempfile.NamedTemporaryFile(delete=False, mode='w') as fp:
            writer: csv.writer = csv.writer(fp)
            merge_n += 1  # increment
            iterators: List[Any] = [
                make_iterator(filename) for filename in merge_filenames
            ]

            # Staggers are used to merge several values into one long value.
            # To not have different values collide they are all multipiled by 100000000 to the power of x.
            staggers = list(reversed(range(len(merge_idxs))))
            staggers = list(map(lambda x: 100000000**x, staggers))
            # 14560217
            filer = heapq.merge(*iterators, key=lambda x: sum(list(
                map(lambda stagger, merge_idx: int(x[merge_idx])*stagger, staggers, merge_idxs))))
            for row in filer:
                writer.writerow(row)

            sorted_filenames.append(fp.name)

        # these are files to get rid of (don't remove original files)
        extra_filenames: List[str] = list(
            set(merge_filenames) - set(orig_filenames))

        for filename in extra_filenames:
            os.remove(filename)
        pbar.update()
    pbar.update()  # final update to reach 100%

    final_filename: str = sorted_filenames[0]

    return final_filename


def make_iterator(filename):
    with open(filename, newline='') as fp:
        count = 0
        for row in csv.reader(fp):
            if count == 0:
                count = 1  # just make it not 0
                continue   # skip header
            elif len(row) == 0:
                continue
            yield row


def get_header(filename) -> List[str]:
    with open(filename, newline='') as fp:
        reader = csv.reader(fp, delimiter=',')
        header: List[str] = next(reader)

    return header


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser: ArgumentParser = ArgumentParser()
    parser.add_argument(
        'csv_dir',
        type=str,
        # default= f"C:\Users\maxar\OneDrive\Documents\Chalmers\ExJobb\Git\Blacklisting\notebooks\data\transactions",
        help='path to directory of csvs',
    )
    parser.add_argument(
        '--merge-only',
        action='store_true',
        default=False,
        help='assume csv_dir contains sorted files'
    )
    parser.add_argument(
        '--sort-only',
        action='store_true',
        default=False,
        help='sort files only'
    )
    parser.add_argument(
        '--sort-columns',
        '--list',
        nargs='+',
        help='What columns to sort on.',
        default=['block_number', 'transaction_index']
        # required=True
    )

    # parser.add_argument(
    #    '--sort-column',
    #    type=str,
    #    default='block_number',
    # )
    parser.add_argument(
        '--out-filename',
        type=str,
        default='transactions-sorted.csv',
    )
    args: Any = parser.parse_args()

    main(args)
    # haha
