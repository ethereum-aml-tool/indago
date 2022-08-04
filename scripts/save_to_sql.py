'''
Used to save data from a CSV file to the given SQL table.
WARNING: SLOW! Only use for smaller CSV files.

For big CSV files, use psql shell with something like the following:
psql -U postgres

\timing
ALTER TABLE seniority SET UNLOGGED;
DROP INDEX ix_seniority_address;
\copy seniority FROM '/data/blacklist/seniority/seniority-tornado-result.csv' delimiter ',' csv header;
# RECREATE NEEDED INDEXES
ALTER TABLE seniority SET LOGGED;
'''
import pandas as pd
from typing import Any, List
from tqdm import tqdm


from db.sql.database import engine


def main(args: Any):
    csv_file: str = args.csv_file
    table_name: str = args.table_name
    columns: List[str] = args.columns
    wei_to_eth: str = args.wei_to_eth

    print(
        f'Saving content from: {csv_file} in table: {table_name}, selected columns={columns}.')
    if wei_to_eth:
        print(f'Converting wei to eth in column: {wei_to_eth}.')

    df = pd.read_csv(csv_file, chunksize=100000, usecols=columns)
    for chunk in tqdm(df, desc="Uploading 100,000 rows per iteration"):
        if wei_to_eth:
            chunk[wei_to_eth] = chunk[wei_to_eth].apply(
                lambda x: int(x) / 10**18)
        chunk.dropna(subset=['address']).to_sql(table_name, engine, if_exists='append', index=False)
    print(f'Finshed uploading to SQL Table {table_name}.')


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser: ArgumentParser = ArgumentParser()
    parser.add_argument(
        'csv_file',
        type=str,
        help='path to csv to upload',
    )
    parser.add_argument(
        '--table-name',
        type=str,
        help='name of table to upload to',
    )
    parser.add_argument(
        '--wei-to-eth',
        type=str,
        help="column to transform from wei to eth",
        default=None
    )
    parser.add_argument(
        '--columns',
        nargs='+',
        help='which columns from csv to upload',
        default=[]
    )
    args: Any = parser.parse_args()

    main(args)
