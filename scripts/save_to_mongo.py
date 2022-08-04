import pandas as pd
from typing import Any, List
from tqdm import tqdm


from db.mongo.database import IndagoSession

# ./data/blacklist_results/haircut.csv --table-name haircut --wei-to-eth balance --columns address balance taint
# ./data/blacklist_results/fifo.csv --table-name fifo --wei-to-eth tainted --columns address tainted untainted
# ./data/blacklist_results/poison.csv --table-name poison --columns address blacklisted
# ./data/blacklist_results/seniority.csv --table-name seniority --wei-to-eth tainted_balance --columns address tainted_balance

def main(args: Any):
    csv_file: str = args.csv_file
    table_name: str = args.table_name
    columns: List[str] = args.columns
    wei_to_eth: str = args.wei_to_eth

    print(
        f'Saving content from: {csv_file} in table: {table_name}, selected columns={columns}.')
    if wei_to_eth:
        print(f'Converting wei to eth in column: {wei_to_eth}.')

    df = pd.read_csv(csv_file, chunksize=1000, usecols=columns)
    for chunk in tqdm(df, desc="Uploading 1000 rows per iteration"):
        if wei_to_eth:
            chunk[wei_to_eth] = chunk[wei_to_eth].apply(
                lambda x: int(x) / 10**18)
        # chunk.to_sql(table_name, engine, if_exists='append', index=False)
        IndagoSession[table_name].insert_many(chunk.rename(columns={'address': '_id'}).to_dict(orient='records'))
    print(f'Finshed uploading to MONGO collection {table_name}.')


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
