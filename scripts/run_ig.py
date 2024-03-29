"""
To scale the run_deposit.py script, we had to forgo creating 
NX graph in memory. This script does exactly that. The motivation
is to isolate the high memory parts to a single file.

Input: user_clusters.json, exchange_clusters.json,

IGNORE COMMENTS BELOW!
mkdir -p /data/dar
gsutil -m cp gs://eth-aml-data/dar-new/data-pruned.csv /data/dar/data-pruned.csv

python3 scripts/run_ig.py /data/dar/data-pruned.csv /data/dar && \
gsutil -m cp /data/dar/user_clusters.json gs://eth-aml-data/dar-fixed/user_clusters.json && \
gsutil -m cp /data/dar/exchange_clusters.json gs://eth-aml-data/dar-fixed/exchange_clusters.json && \
gsutil -m cp /data/dar/user_clusters_map.json gs://eth-aml-data/dar-fixed/user_clusters_map.json && \
gsutil -m cp /data/dar/exchange_clusters_map.json gs://eth-aml-data/dar-fixed/exchange_clusters_map.json

gsutil -m cp gs://eth-aml-data/dar-fixed/user_clusters.json . && \
gsutil -m cp gs://eth-aml-data/dar-fixed/user_clusters_map.json .
"""

import os
import itertools
from pprint import pprint
import numpy as np
import pandas as pd
import networkx as nx
import igraph as ig
from tqdm import tqdm
from sqlalchemy import true
from utils.utils import to_json, from_json
from typing import Any, Dict, List, Set, Tuple


def main(args: Any):
    data: pd.DataFrame = pd.read_csv(args.data_file)

    if not os.path.isdir(args.save_dir):
        os.makedirs(args.save_dir)

    print("making user graph...", end="", flush=True)
    user_graph: ig.Graph = make_graph(data.user, data.deposit)

    """ NOTE: These should not be necessary in any of the current use cases. """
    # if args.gas_price_file:
    #     gas_price_sets: List[Set[str]] = from_json(args.gas_price_file)
    #     print('adding gas price nodes...', end = '', flush=True)
    #     user_graph: nx.DiGraph = add_to_user_graph(user_graph, gas_price_sets)

    # if args.multi_denom_file:
    #     multi_denom_sets: List[Set[str]] = from_json(args.multi_denom_file)
    #     print('adding multi denom nodes...', end = '', flush=True)
    #     user_graph: nx.DiGraph = add_to_user_graph(user_graph, multi_denom_sets)

    print("making exchange graph...", end="", flush=True)
    exchange_graph: ig.Graph = make_graph(data.deposit, data.exchange)

    # TODO: Make get_wcc() usage optional through flag.
    print("making user wcc...", end="", flush=True)
    # user_wccs: List[Set[str]] = get_wcc(user_graph)
    user_wccs, user_map = get_wcc_graphs(user_graph)
    print("saving user wcc...", end="", flush=True)
    to_json(
        user_wccs,
        os.path.join(args.save_dir, "user_clusters.json"),
        line_delimited=False,
        with_pandas=True,
    )
    to_json(
        user_map,
        os.path.join(args.save_dir, "user_clusters_map.json"),
        line_delimited=False,
        with_pandas=True,
    )
    del user_wccs, user_map

    # algorithm 1 line 13
    # We actually want to keep this information!
    # user_wccs: List[Set[str]] = self._remove_deposits(
    #     user_wccs,
    #     set(store.deposit.to_numpy().tolist()),
    # )

    print("making exchange wcc...", end="", flush=True)
    # exchange_wccs: List[Set[str]] = get_wcc(exchange_graph)
    exchange_wccs, exchange_map = get_wcc_graphs(exchange_graph)
    print("saving exchange wcc...", end="", flush=True)
    to_json(
        exchange_wccs,
        os.path.join(args.save_dir, "exchange_clusters.json"),
        line_delimited=False,
        with_pandas=True,
    )
    to_json(
        exchange_map,
        os.path.join(args.save_dir, "exchange_clusters_map.json"),
        line_delimited=False,
        with_pandas=True,
    )
    del exchange_wccs, exchange_map

    # # prune trivial clusters
    # user_wccs: List[Set[str]] = remove_singletons(user_wccs)
    # exchange_wccs: List[Set[str]] = remove_singletons(exchange_wccs)


def add_to_user_graph(graph: nx.DiGraph, clusters: List[Set[str]]):
    for cluster in clusters:
        assert len(cluster) == 2, "Only supports edges with two nodes."
        node_a, node_b = cluster
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.add_edge(node_a, node_b)
    return graph


def get_wcc(graph: ig.Graph) -> List[Set[str]]:
    clusters: ig.VertexClustering = graph.clusters(mode="weak")
    addresses = clusters.graph.vs["name"]

    cluster_sets: List[Set[str]] = []
    for c in clusters:
        tmp_cluster: Set[str] = set()
        for entry in c:
            tmp_cluster.add(addresses[entry])
        cluster_sets.append(tmp_cluster)

    return cluster_sets


def get_wcc_graphs(
    graph: ig.Graph,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    get_wcc() but also returns information about the edges
    and a mapping between addresses and their cluster.

    Can be used to construct accurate subgraphs.
    """
    clusters: ig.VertexClustering = graph.clusters(mode="weak")
    graph_dicts: List[Dict[str, Any]] = []
    address_map: List[Dict[str, Any]] = []
    for subgraph in tqdm(clusters.subgraphs()):
        # ignore clusters with only one edge or less than three nodes
        if subgraph.ecount() <= 1 or subgraph.vcount() <= 2:
            continue
        if subgraph.vcount() > 1000000:
            print(f"{len(subgraph.vs)} nodes in big subgraph")
            continue
        db_id = len(graph_dicts)
        graph_dict = {"_id": db_id, "nodes": [], "edges": []}
        for node in subgraph.vs:
            address = node["name"]
            map_dict = {"_id": address, "cluster_id": db_id}
            address_map.append(map_dict)
            graph_dict["nodes"].append(address)
        unique_edges = remove_duplicate_edges(subgraph.es)
        for edge in unique_edges:
            graph_dict["edges"].append([edge.source, edge.target])
        graph_dicts.append(graph_dict)

    return graph_dicts, address_map


def remove_duplicate_edges(edges):
    """
    Remove duplicate edges from a list of edges.
    Only takes source and target into account.
    """
    unique_edges = []
    unique_values = {}
    for edge in edges:
        from_to = f"{edge.source}-{edge.target}"
        if from_to not in unique_values:
            unique_edges.append(edge)
            unique_values[f"{from_to}"] = True

    return unique_edges


def remove_deposits(components: List[Set[str]], deposit: Set[str]):
    # remove all deposit addresses from wcc list
    new_components: List[Set[str]] = []
    for component in components:
        new_component: Set[str] = component - deposit
        new_components.append(new_component)

    return new_components


def remove_singletons(components: List[Set[str]]):
    # remove clusters with just one entity... these are not interesting.
    return [c for c in components if len(c) > 1]


def make_graph(node_a: pd.Series, node_b: pd.Series) -> ig.Graph:
    """
    TODO: Test memory usage. Might have to optimize or go for
    another approach entirely.
    """
    assert node_a.size == node_b.size, "Dataframes are uneven sizes."
    unique_a: pd.DataFrame = pd.DataFrame()
    unique_a["address"] = node_a.unique()
    unique_b: pd.DataFrame = pd.DataFrame()
    unique_b["address"] = node_b.unique()

    vertices: pd.DataFrame = pd.concat([unique_a, unique_b], axis=0)
    vertices.rename(columns={0: "address"}, inplace=True)
    edges: pd.DataFrame = pd.concat([node_a, node_b], axis=1)

    # TypeError: Source and target IDs must be 0-based integers, found types [dtype('O'), dtype('O')]
    vertices["id"] = vertices["address"].astype("category").cat.codes
    edges["from"] = node_a.astype("category").cat.codes
    edges["to"] = node_b.astype("category").cat.codes
    graph: ig.Graph = ig.Graph.DataFrame(edges, directed=True, vertices=vertices)

    return graph


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("data_file", type=str, help="path to cached out of deposit.py")
    parser.add_argument("save_dir", type=str, help="where to save files.")
    # parser.add_argument('--gas_price_file', default=None,
    #                     type=str, help='path to gas price address sets')
    # parser.add_argument('--multi_denom_file', default=None,
    #                     type=str, help='path to gas price address sets')
    args: Any = parser.parse_args()

    main(args)
