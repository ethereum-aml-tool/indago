from typing import Any, Optional
import pandas as pd
import numpy as np
from tqdm import tqdm
from pprint import pprint
from db.mongo.database import IndagoSession
from db.mongo.schemas import DARAddressMapping, DARGraph


async def main(args: Any):
    blacklisting_df = pd.read_csv(args.blacklisting_csv)
    title = args.algorithm

    # MONGO
    db = IndagoSession
    dar_collection = db['dar']
    dar_map_collection = db['dar_map']

    def filter_exchange_nodes(nodes, edges) -> list[str]:
        '''
        Calculates non-exchange nodes from graph data.
        '''
        non_exchange_nodes = []
        for edge in edges:
            # print(edge)
            non_exchange_nodes.append(nodes[edge[0]])

        return set(non_exchange_nodes)

    async def process_graphs(batch_size: int = 10000, max_graphs: Optional[int] = None):
        '''
        Prints a bunch of statistics about the number of nodes in the DAR graphs.

        returns: numpy.array with the number of nodes in each graph
        '''
        n_graphs = await dar_collection.count_documents({})
        print(f'INFO: {n_graphs:,} DAR graphs in collection')
        if max_graphs is None:
            max_graphs = n_graphs

        node_counts = []
        graphs_with_blacklisted = []
        total_nodes_in_blacklist = 0

        i = 0
        print(
            f'INFO: Fetching {max_graphs:,} graphs, batch_size={batch_size:,}...')
        pbar = tqdm(total=max_graphs)
        while i * batch_size < max_graphs + batch_size:
            cursor = dar_collection.find()
            cursor.skip(i * batch_size)
            for graph in await cursor.to_list(length=batch_size):
                n_nodes = len(graph['nodes'])
                node_counts.append(n_nodes)

                nodes_in_blacklist = 0
                for address in filter_exchange_nodes(graph['nodes'], graph['edges']):
                    try:
                        row = blacklisting_df.loc[address]
                        # print(f'{row}')
                        if row['taint'] > 0:
                            nodes_in_blacklist += 1
                            if nodes_in_blacklist == 1:
                                graphs_with_blacklisted.append(graph)
                        del row
                    except KeyError:
                        pass
                total_nodes_in_blacklist += nodes_in_blacklist

            pbar.update(batch_size)
            i += 1
        pbar.close()

        print(f'DONE: {sum(node_counts):,} total nodes (addresses)')
        print(f'STATS: average={(np.mean(node_counts)):.2f} nodes')
        print(f'STATS: median={(np.median(node_counts)):.2f} nodes')
        print(f'STATS: min={(np.min(node_counts))}')
        print(f'STATS: max={(np.max(node_counts)):,}')
        print(f'STATS: standard deviation={(np.std(node_counts)):.2f}')
        print(f'STATS: variance={(np.var(node_counts)):.2f}')
        print(f'\n---{title} (TORNADO)---')
        print(
            f'STATS: nodes cross found={total_nodes_in_blacklist:,}, percentage of total={(total_nodes_in_blacklist / sum(node_counts) * 100):.2f}%')
        print(
            f'STATS: graphs with >0 nodes in {title}={len(graphs_with_blacklisted):,}, percentage of total={(len(graphs_with_blacklisted) / n_graphs * 100):.2f}%')

        return np.array(node_counts), graphs_with_blacklisted

    node_counts, sen_graphs = await process_graphs(batch_size=50000, max_graphs=None)

    print(args)


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser: ArgumentParser = ArgumentParser()

    parser.add_argument('algorithm', type=str,
                        help='algorithm to run cluster data against (poison, haircut, fifo, seniority), used for titles')
    parser.add_argument('blacklisting_csv', type=str,
                        help='path to result file from blacklisting')
    parser.add_argument('output_directory', type=str,
                        help='where to save the output')

    args: Any = parser.parse_args()

    main(args)
