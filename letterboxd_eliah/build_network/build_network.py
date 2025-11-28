"""
Letterboxd Network Builder

Builds a NetworkX directed graph from Letterboxd data.
"""

import networkx as nx
import pickle
import logging
from pathlib import Path
from typing import Dict
from .letterboxd_parser import LetterboxdDataParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_letterboxd_network(parser: LetterboxdDataParser) -> nx.DiGraph:
    """
    Build a NetworkX directed graph from Letterboxd data.

    The graph has the following properties:
    - Nodes: Users (only exported users with complete data)
    - Edges: Directed follows (A -> B means "A follows B")
    - Node attributes: username, followers_count, reviews

    Args:
        parser: LetterboxdDataParser instance

    Returns:
        NetworkX DiGraph with user nodes and follow edges
    """
    logger.info("Building Letterboxd network...")

    # Load all user data
    all_data = parser.get_all_user_data()
    exported_users = set(all_data.keys())

    logger.info(f"Creating graph with {len(exported_users)} users")

    # Create directed graph
    G = nx.DiGraph()

    # Add nodes with attributes
    logger.info("Adding nodes with attributes...")
    for username, data in all_data.items():
        G.add_node(
            username,
            username=username,
            followers_count=data["followers_count"],
            reviews=data["reviews"]
        )

    logger.info(f"Added {G.number_of_nodes()} nodes")

    # Add edges (follows)
    logger.info("Adding edges (follows)...")
    edge_count = 0
    skipped_edges = 0

    for username, data in all_data.items():
        following = data["following"]

        for followed_username in following.keys():
            # Only add edge if the followed user is also in our exported users
            if followed_username in exported_users:
                G.add_edge(username, followed_username)
                edge_count += 1
            else:
                skipped_edges += 1

    logger.info(f"Added {edge_count} edges")
    logger.info(f"Skipped {skipped_edges} edges to non-exported users")

    # Log graph statistics
    logger.info("=" * 50)
    logger.info("Network Statistics:")
    logger.info(f"  Nodes: {G.number_of_nodes()}")
    logger.info(f"  Edges: {G.number_of_edges()}")
    logger.info(f"  Average degree: {sum(dict(G.degree()).values()) / G.number_of_nodes():.2f}")
    logger.info(f"  Average in-degree: {sum(dict(G.in_degree()).values()) / G.number_of_nodes():.2f}")
    logger.info(f"  Average out-degree: {sum(dict(G.out_degree()).values()) / G.number_of_nodes():.2f}")
    logger.info("=" * 50)

    return G


def save_network(graph: nx.DiGraph, output_path: str = "letterboxd_network.pickle") -> None:
    """
    Save the network graph to a pickle file.

    Args:
        graph: NetworkX DiGraph to save
        output_path: Path where to save the pickle file
    """
    output_file = Path(output_path)

    logger.info(f"Saving network to {output_file}...")

    try:
        with open(output_file, 'wb') as f:
            pickle.dump(graph, f)

        file_size = output_file.stat().st_size / (1024 * 1024)  # MB
        logger.info(f"Network saved successfully! File size: {file_size:.2f} MB")
    except Exception as e:
        logger.error(f"Error saving network: {e}")
        raise


def load_network(input_path: str = "letterboxd_network.pickle") -> nx.DiGraph:
    """
    Load a network graph from a pickle file.

    Args:
        input_path: Path to the pickle file

    Returns:
        NetworkX DiGraph loaded from file
    """
    input_file = Path(input_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Network file not found: {input_file}")

    logger.info(f"Loading network from {input_file}...")

    try:
        with open(input_file, 'rb') as f:
            graph = pickle.load(f)

        logger.info(f"Network loaded successfully!")
        logger.info(f"  Nodes: {graph.number_of_nodes()}")
        logger.info(f"  Edges: {graph.number_of_edges()}")

        return graph
    except Exception as e:
        logger.error(f"Error loading network: {e}")
        raise
