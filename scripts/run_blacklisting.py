from typing import Any, Dict, List, Set

import pandas as pd
from src.blacklist.fifo_blacklister import FIFO
from utils.loader import DataframeLoader
from src.blacklist.poison_blacklister import Poison
from src.blacklist.haircut_blacklister import Haircut
# TODO Which FIFO algorithm to use?
from src.blacklist.advanced_fifo_blacklister import AdvancedFIFO
from src.blacklist.seniority_blacklister import Seniority


def main(args: Any):
    print('\ncreating data loader with args:')
    print(f'block_csv: {args.blocks_csv}')
    print(f'traces_csv: {args.traces_csv}')
    print(f'tornado_csv: {args.tornado_csv}')
    print(f'known_addresses_csv: {args.known_addresses_csv}')
    print(f'save_dir: {args.save_dir}')

    loader: DataframeLoader = DataframeLoader(
        block_csv=args.blocks_csv,
        known_addresses_csv=args.known_addresses_csv,
        cache_dir=args.save_dir,
        traces_csv=args.traces_csv,
        tornado_csv=args.tornado_csv,
    )

    if args.only_tornado:
        blacklist: pd.Series = loader.get_tornado().address
    elif args.only_flagged:
        blacklist: pd.Series = loader.get_flagged().address
    else:
        tornado: pd.Dataframe = loader.get_tornado()
        flagged: pd.Dataframe = loader.get_flagged()
        blacklist: pd.Series = pd.concat([tornado.address, flagged.address])
        blacklist = blacklist.reset_index(drop=True)

    # TODO Python >= 3.10 supports match-case statements, more elegant
    if args.algorithm == 'poison':
        algo = Poison(
            loader,
            initial_blacklist=blacklist,
            save_dir=args.save_dir
        )
    elif args.algorithm == 'haircut':
        algo = Haircut(
            loader,
            initial_blacklist=blacklist,
            save_dir=args.save_dir
        )
    elif args.algorithm == 'fifo':
        algo = AdvancedFIFO(
            loader,
            initial_blacklist=blacklist,
            save_dir=args.save_dir
        )
    elif args.algorithm == 'fifo-simple':
        algo = FIFO(
            loader,
            initial_blacklist=blacklist,
            save_dir=args.save_dir
        )
    elif args.algorithm == 'seniority':
        algo = Seniority(
            loader,
            initial_blacklist=blacklist,
            save_dir=args.save_dir
        )
    else:
        raise ValueError(f'unknown algorithm: {args.algorithm}')

    algo.make_blacklist(start_block=args.start_block, end_block=args.end_block)

    # if args.to_eth:
    #     raise NotImplementedError('to_eth not implemented yet')
    #     for chunk in pd.read_csv(f'{args.save_dir}/{args.algorithm}-result.csv', chunksize=100000):
    #         yield chunk

    print(f'[DONE] Blacklisting with {args.algorithm} algorithm.')


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser: ArgumentParser = ArgumentParser()
    parser.add_argument('algorithm', type=str,
                        help='algorithm to use (poison, haircut, fifo, fifo-simple, seniority)')
    parser.add_argument('blocks_csv', type=str, help='path to block data')
    parser.add_argument('traces_csv', type=str,
                        help='path to transaction data')
    parser.add_argument('known_addresses_csv', type=str,
                        help='path to known address data')
    parser.add_argument('tornado_csv', type=str,
                        help='path to tornado addresses')
    parser.add_argument('save_dir', type=str, help='path to save output')
    parser.add_argument('--start-block', type=int, default=None)
    parser.add_argument('--end-block', type=int, default=None)
    parser.add_argument('--to-eth', action='store_true',
                        default=False, help='convert wei to eth')
    parser.add_argument('--only-tornado', action='store_true',
                        default=False, help='only run with tornado addresses')
    parser.add_argument('--only-flagged', action='store_true', default=False,
                        help='only run with flagged addresses from known_addresses.csv')

    args: Any = parser.parse_args()

    main(args)
