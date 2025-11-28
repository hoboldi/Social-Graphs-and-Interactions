"""
Main script to build Letterboxd network from exported data.

Usage:
    python main.py
"""

import logging
from .letterboxd_parser import LetterboxdDataParser
from .build_network import build_letterboxd_network, save_network

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function to build and save the Letterboxd network."""
    try:
        # 1. Initialize Parser
        logger.info("Initializing parser...")
        parser = LetterboxdDataParser("../exports")

        # 2. Build Network
        logger.info("Building network...")
        graph = build_letterboxd_network(parser)

        # 3. Print Statistics
        print("\n" + "=" * 60)
        print("LETTERBOXD NETWORK BUILT SUCCESSFULLY")
        print("=" * 60)
        print(f"Total Nodes (Users): {graph.number_of_nodes()}")
        print(f"Total Edges (Follows): {graph.number_of_edges()}")
        print(f"Average Degree: {sum(dict(graph.degree()).values()) / graph.number_of_nodes():.2f}")
        print("=" * 60 + "\n")

        # 4. Save as Pickle
        save_network(graph, "../letterboxd_network.pickle")
        print("✓ Network saved to letterboxd_network.pickle")

        # 5. Print usage example
        print("\n" + "=" * 60)
        print("HOW TO USE THE NETWORK")
        print("=" * 60)
        print("import pickle")
        print("import networkx as nx")
        print("")
        print("# Load the network")
        print("with open('letterboxd_network.pickle', 'rb') as f:")
        print("    G = pickle.load(f)")
        print("")
        print("# Get node data")
        print("node_data = G.nodes['aadowd']")
        print("print(f\"Followers: {node_data['followers_count']}\")")
        print("print(f\"Reviews: {len(node_data['reviews'])}\")")
        print("")
        print("# Get degree")
        print("print(f\"Following: {G.out_degree('aadowd')}\")")
        print("print(f\"Followed by (in network): {G.in_degree('aadowd')}\")")
        print("=" * 60 + "\n")

    except Exception as e:
        logger.error(f"Error building network: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
