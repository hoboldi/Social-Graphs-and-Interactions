"""
Letterboxd Data Parser

Parses exported Letterboxd user data from JSON files.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LetterboxdDataParser:
    """Parser for Letterboxd exported data."""

    def __init__(self, exports_path: str = "exports"):
        """
        Initialize the parser.

        Args:
            exports_path: Path to the exports directory
        """
        self.exports_path = Path(exports_path)
        self.users_path = self.exports_path / "users"
        self.state_path = self.exports_path / "_state"

        if not self.exports_path.exists():
            raise FileNotFoundError(f"Exports path not found: {self.exports_path}")

    def get_exported_users(self) -> List[str]:
        """
        Get list of exported usernames from exported.json.

        Returns:
            List of usernames that have been successfully exported
        """
        exported_file = self.state_path / "exported.json"

        if not exported_file.exists():
            logger.warning(f"exported.json not found at {exported_file}")
            return []

        try:
            with open(exported_file, 'r', encoding='utf-8') as f:
                users = json.load(f)
            logger.info(f"Found {len(users)} exported users")
            return users
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing exported.json: {e}")
            return []

    def load_user_following(self, username: str) -> Dict:
        """
        Load following.json for a specific user.

        Args:
            username: The username to load following data for

        Returns:
            Dictionary with following data, empty dict if not found
        """
        following_file = self.users_path / username / "following.json"

        if not following_file.exists():
            logger.warning(f"following.json not found for user {username}")
            return {}

        try:
            with open(following_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing following.json for {username}: {e}")
            return {}

    def load_user_metadata(self, username: str) -> Dict:
        """
        Load followers_count.json for a specific user.

        Args:
            username: The username to load metadata for

        Returns:
            Dictionary with metadata (username, followers_count)
        """
        metadata_file = self.users_path / username / "followers_count.json"

        if not metadata_file.exists():
            logger.warning(f"followers_count.json not found for user {username}")
            return {"username": username, "followers_count": 0}

        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing followers_count.json for {username}: {e}")
            return {"username": username, "followers_count": 0}

    def load_user_reviews(self, username: str) -> Dict:
        """
        Load reviews.json for a specific user.

        Args:
            username: The username to load reviews for

        Returns:
            Dictionary with reviews data, empty dict if not found
        """
        reviews_file = self.users_path / username / "reviews.json"

        if not reviews_file.exists():
            logger.warning(f"reviews.json not found for user {username}")
            return {}

        try:
            with open(reviews_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Return the reviews dict, handle both formats
                if isinstance(data, dict) and "reviews" in data:
                    return data["reviews"]
                return data
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing reviews.json for {username}: {e}")
            return {}

    def get_all_user_data(self) -> Dict:
        """
        Load all data for all exported users.

        Returns:
            Dictionary with structure:
            {
                "username": {
                    "followers_count": int,
                    "following": dict,
                    "reviews": dict
                }
            }
        """
        users = self.get_exported_users()
        all_data = {}

        logger.info(f"Loading data for {len(users)} users...")

        for i, username in enumerate(users, 1):
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(users)} users loaded")

            metadata = self.load_user_metadata(username)
            following = self.load_user_following(username)
            reviews = self.load_user_reviews(username)

            all_data[username] = {
                "followers_count": metadata.get("followers_count", 0),
                "following": following,
                "reviews": reviews
            }

        logger.info(f"Successfully loaded data for {len(all_data)} users")
        return all_data
