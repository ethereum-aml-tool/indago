"""
To scale the run_deposit.py script, we had to forgo creating 
NX graph in memory. This script does exactly that. The motivation
is to isolate the high memory parts to a single file.
"""

from enum import unique
import os
from xmlrpc.client import Boolean
import numpy as np
import pandas as pd
import igraph as ig
import matplotlib.pyplot as plt
from typing import Any, List, Tuple


# TODO Look at https://git.skewed.de/count0/graph-tool/-/wikis/installation-instructions#debian-ubuntu
def main(args: Any):
    data: pd.DataFrame = pd.read_csv(args.data_file, nrows=1000)
    print(f'shape of input data: {data.shape}')

    print('making user graph...')
    graph = make_graph(data)
    print('creating clusters...')
    components: ig.VertexClustering = graph.clusters(mode="weak")
    print('plotting clusters...')
    subgraph: ig.Graph = components.subgraph(1)
    print(subgraph.vs['name'])
    for e in subgraph.es:
        print(e.tuple)
    #plot_graph(components)
    #giant: ig.Graph = components.giant()
    #plot_graph(giant, giant.vs['name'])


def make_graph(df: pd.DataFrame) -> ig.Graph:
    """
    DEPRECATED: This assumes we can store all connections in memory.

    Make a directed graph connecting each row of node_a to the 
    corresponding row of node_b.
    """
    user: pd.Series = df.user
    deposit: pd.Series = df.deposit

    assert user.size == deposit.size, "Dataframes are uneven sizes."
    unique_users: pd.DataFrame = pd.DataFrame()
    unique_users['address'] = user.unique()
    unique_users['type'] = 'user'
    unique_deposits: pd.DataFrame = pd.DataFrame()
    unique_deposits['address'] = deposit.unique()
    unique_deposits['type'] = 'deposit'

    vertices: pd.DataFrame = pd.concat([unique_users, unique_deposits], axis=0)
    #del unique_users, unique_deposits
    vertices.rename(columns={0: 'address'}, inplace=True)
    edges: pd.DataFrame = pd.concat([df.user, df.deposit, df.conf], axis=1)

    graph: ig.Graph = ig.Graph.DataFrame(
        edges, directed=True, vertices=vertices)

    return graph


def plot_graph(graph: ig.Graph, display_labels: Boolean = False, save_path: str = None):
    components = graph
    if type(graph) is not ig.VertexClustering:
        components = graph.clusters(mode="weak")

    def type_to_color(type_: str) -> str:
        if type_ == 'user':
            return 'blue'
        elif type_ == 'deposit':
            return 'red'
        else:
            return 'black'

    fig, ax = plt.subplots()
    ig.plot(
        components,
        target=ax if save_path is None else save_path,
        palette=ig.RainbowPalette(),
        vertex_size=7,
        #vertex_color=list(map(int, ig.rescale(components.membership, (0, 200), clamp=True))),
        vertex_color=list(map(type_to_color, components.graph.vs['type'])),
        vertex_label=components.graph.vs['name'] if display_labels else None,
        edge_width=0.7,
    )

    if save_path is None:
        plt.show()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('data_file', type=str,
                        help='path to cached out of deposit.py')
    parser.add_argument('save_dir', type=str, help='where to save files.')
    args: Any = parser.parse_args()

    main(args)
