# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Letterboxd data export repository** containing scraped user data from letterboxd.com (film social network). The data appears to be collected for social graph analysis or research purposes.

## Data Structure

### Directory Layout

```
exports/
├── _state/                    # Crawling/export state management
│   ├── enqueued.json         # Queue of usernames to be scraped (~76k lines)
│   ├── exported.json         # List of successfully exported usernames (~642 users)
│   └── pq.json               # Priority queue with [priority, username, url] tuples
└── users/                     # Exported user data (one directory per user)
    └── {username}/
        ├── followers_count.json   # Simple count: {"username": "...", "followers_count": 12947}
        ├── following.json         # Detailed following data (nested objects with user profiles)
        └── reviews.json          # Movie reviews with ratings, dates, content
```

### Data Formats

**followers_count.json**: Simple count object
```json
{
  "username": "aadowd",
  "followers_count": 12947
}
```

**following.json**: Nested dictionary keyed by username with full user profiles including avatar URLs, stats (followers, following, watched, lists, likes)

**reviews.json**: Nested dictionary keyed by review ID containing movie metadata, ratings (0-10), review content, dates, and pagination info

**pq.json**: Priority queue entries as arrays: `[priority_number, "username", "https://letterboxd.com/username/"]`

### State Management

- **enqueued.json**: Array of usernames queued for scraping
- **exported.json**: Array of usernames successfully exported
- **pq.json**: Priority queue for crawl ordering (negative priorities used)

## Notes

- No source code (Python scripts, notebooks, etc.) exists in this directory - only exported data
- The actual scraping/crawling code likely resides elsewhere or has been removed
- This appears to be assignment/coursework data (parent directory: `SGI/assignments/`)
- Total of ~642 exported users with full data
- ~76k users in the queue waiting to be processed
