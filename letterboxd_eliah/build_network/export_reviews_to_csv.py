import json
import csv
import os
from pathlib import Path


def clean_text(text):
    """
    Remove unusual line terminators and other problematic Unicode characters.

    Args:
        text: The text to clean

    Returns:
        Cleaned text with unusual characters replaced by spaces
    """
    if not text:
        return text

    # Replace Line Separator (U+2028) and Paragraph Separator (U+2029) with spaces
    text = text.replace('\u2028', ' ').replace('\u2029', ' ')

    # Replace other potential problematic characters
    text = text.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')

    return text


def export_reviews_to_csv(users_dir=None, output_file=None):
    """
    Export all user reviews to a single CSV file.

    Args:
        users_dir: Path to the users directory
        output_file: Path to the output CSV file
    """
    # Get script directory
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent

    # Set default paths relative to project directory
    if users_dir is None:
        users_dir = project_dir / 'exports' / 'users'
    if output_file is None:
        output_file = project_dir / 'reviews.csv'

    users_path = Path(users_dir)

    # Prepare CSV file
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['review_id', 'username_of_creator', 'moviename', 'rating', 'review_content', 'date_of_review']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Counter for progress
        user_count = 0
        review_count = 0
        # test
        # Iterate through all user directories
        for user_dir in sorted(users_path.iterdir()):
            if not user_dir.is_dir():
                continue

            username = user_dir.name
            reviews_file = user_dir / 'reviews.json'

            # Skip if reviews.json doesn't exist
            if not reviews_file.exists():
                continue

            # Read reviews.json
            try:
                with open(reviews_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # Extract reviews
                reviews = data.get('reviews', {})

                for review_id, review_data in reviews.items():
                    # Extract movie name
                    moviename = clean_text(review_data.get('movie', {}).get('name', ''))

                    # Extract rating (can be None if not rated)
                    rating = review_data.get('rating', '')

                    # Extract review content
                    review_content = clean_text(review_data.get('review', {}).get('content', ''))

                    # Extract date
                    date_obj = review_data.get('date', {})
                    year = date_obj.get('year', '')
                    month = date_obj.get('month', '')
                    day = date_obj.get('day', '')
                    if year and month and day:
                        try:
                            date_of_review = f"{year}-{int(month):02d}-{int(day):02d}"
                        except (ValueError, TypeError):
                            date_of_review = ''
                    else:
                        date_of_review = ''

                    # Write row to CSV
                    writer.writerow({
                        'review_id': review_id,
                        'username_of_creator': username,
                        'moviename': moviename,
                        'rating': rating,
                        'review_content': review_content,
                        'date_of_review': date_of_review
                    })

                    review_count += 1

                user_count += 1
                if user_count % 50 == 0:
                    print(f"Processed {user_count} users, {review_count} reviews...")

            except json.JSONDecodeError as e:
                print(f"Error reading {reviews_file}: {e}")
                continue
            except Exception as e:
                print(f"Error processing {username}: {e}")
                continue

        print(f"\nDone! Processed {user_count} users and exported {review_count} reviews to {output_file}")


if __name__ == '__main__':
    export_reviews_to_csv()
