import asyncio
from typing import Any, List, Optional
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm
from pprint import pprint
from db.mongo.database import IndagoSession
from db.mongo.schemas import DARAddressMapping, DARGraph


def filter_exchange_nodes(nodes, edges) -> list[str]:
    '''
    Calculates non-exchange nodes from graph data.
    '''
    non_exchange_nodes = []
    for edge in edges:
        # print(edge)
        non_exchange_nodes.append(nodes[edge[0]])

    return set(non_exchange_nodes)


async def main(args: Any):
    print('INFO: Loading blacklisting results into memory...')
    blacklisting_df: pd.DataFrame = pd.read_csv(args.blacklisting_csv, index_col=0)
    pprint(blacklisting_df.head())

    title: str = args.algorithm
    output_path: str = args.output_path

    # MONGO
    db = IndagoSession
    dar_collection = db['dar']
    dar_map_collection = db['dar_map']

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

    print('INFO: Processing clusters...')
    node_counts, bl_graphs = await process_graphs(batch_size=50000, max_graphs=None)

    print('INFO: Generating plots...')
    generate_cluster_graphs(blacklisting_df, node_counts,
                            bl_graphs, title, output_path)

    del blacklisting_df, node_counts, bl_graphs
    print(f"INFO: Done with {title} analysis!")


def generate_cluster_graphs(bl_df: pd.DataFrame, node_counts: np.array, bl_graphs: List, title: str, output_path: str):
    '''
    Generates a histogram of the number of nodes in each graph.
    '''
    def reject_outliers(data, n_deviations=0.3):
        mean = np.mean(data)
        standard_deviation = np.std(data)
        distance_from_mean = abs(data - mean)
        not_outlier = distance_from_mean < n_deviations * standard_deviation
        return data[not_outlier]

    sns.set_style("whitegrid")
    sns.set(rc={'figure.figsize': (15, 8)})

    # Number of nodes in each graph
    p = sns.histplot(data=reject_outliers(node_counts),
                     stat='percent', shrink=8.25)
    p.set_xlabel('Number of nodes/addresses in cluster', fontsize=16)
    p.set_ylabel('Percentage of graphs', fontsize=16)
    p.set_title('Distribution of number of nodes in DAR graphs', fontsize=16)
    p.figure.savefig(f'{output_path}/{title}_cluster_histogram.png')

    # Number of nodes in each graph with >0 nodes in blacklist
    sen_node_counts = np.array([len(graph['nodes']) for graph in bl_graphs])
    p = sns.histplot(data=reject_outliers(sen_node_counts),
                     stat='percent', shrink=4.75)
    p.set_xlabel('Number of nodes/addresses in cluster', fontsize=16)
    p.set_ylabel('Percentage of graphs', fontsize=16)
    p.set_title(
        'Distribution of number of nodes in graphs with >0 flagged nodes', fontsize=16)
    p.figure.savefig(
        f'{output_path}/{title}_cluster_histogram_with_blacklisted.png')
    plt.clf()

    # Flagged vs clean
    flagged_counts = []
    total_counts = []
    for graph in tqdm(bl_graphs):
        flagged = 0
        total = 0
        for address in filter_exchange_nodes(graph['nodes'], graph['edges']):
            total += 1
            try:
                row = bl_df.loc[address]
                flagged += 1
                del row
            except KeyError:
                pass
        flagged_counts.append(flagged)
        total_counts.append(total)

    avg_flagged = np.mean(flagged_counts)
    avg_total = np.mean(total_counts)

    print(f'STATS: average flagged user nodes={avg_flagged:.2f} nodes')
    print(f'STATS: average total user nodes={avg_total:.2f} nodes')
    print(
        f'STATS: num clusters with a single flagged node={flagged_counts.count(1):,}')

    # Flagged vs clean in flagged clusters
    data = [sum(flagged_counts), sum(total_counts) - sum(flagged_counts)]
    labels = ['Flagged', 'Clean']
    colors = sns.color_palette('pastel')[0:5]
    plt.pie(data, labels=labels, colors=colors,
            autopct='%1.1f%%', shadow=True, startangle=90)
    plt.title('Amount of flagged/clean nodes in flagged graphs', fontsize=16)
    plt.savefig(f'{output_path}/{title}_flagged_pie.png')
    plt.clf()

    # Graphs containing clean nodes
    n_unflagged = []
    for i, n in enumerate(flagged_counts):
        n_unflagged.append(total_counts[i] - n)
    clusters_with_clean = list(filter(lambda x: x > 0, n_unflagged))

    data = [len(flagged_counts) - len(clusters_with_clean),
            len(clusters_with_clean)]
    labels = ['All flagged', 'Contains clean nodes']
    colors = sns.color_palette('pastel')[0:5]

    plt.pie(data, labels=labels, colors=colors,
            autopct='%1.1f%%', shadow=True, startangle=90)
    plt.title('Graphs containing clean nodes', fontsize=16)
    plt.savefig(f'{output_path}/{title}_clean_pie.png')
    plt.clf()


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser: ArgumentParser = ArgumentParser()

    parser.add_argument('algorithm', type=str,
                        help='algorithm to run cluster data against (poison, haircut, fifo, seniority), used for titles')
    parser.add_argument('blacklisting_csv', type=str,
                        help='path to result file from blacklisting')
    parser.add_argument('output_path', type=str,
                        help='where to save the output')

    args: Any = parser.parse_args()

    asyncio.run(main(args))
