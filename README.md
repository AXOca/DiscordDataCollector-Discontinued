# Discord Data Collector Bot

## Project Status

This repository is archived and discontinued due to moral considerations regarding data collection and privacy. The code is provided for educational purposes only. If you choose to implement this bot, you must obtain explicit consent from all server members before collecting any data. Consider implementing an opt-in feature to ensure user privacy and compliance with data protection regulations.

## Overview

This Discord bot is a comprehensive analytics and logging solution designed to capture and store a wide range of server activities. It provides server administrators with detailed insights into user interactions, message patterns, moderation actions, and even Spotify listening habits within their Discord community.

## Features

- Extensive Event Logging: Captures a wide array of Discord events including messages, edits, deletions, reactions, voice activity, role changes, and more.
- Moderation Tracking: Logs moderation actions such as bans, timeouts, and channel permission changes.
- User Activity Monitoring: Tracks user presence, status changes, and custom activities.
- Spotify Integration: Records Spotify listening activity of server members.
- Thread and Forum Tracking: Monitors the creation and deletion of threads and forum channels.
- Invite Tracking: Logs the creation and usage of server invites.
- Scheduled Event Logging: Keeps track of server scheduled events.
- Emoji and Sticker Changes: Records additions, removals, and updates to server emojis and stickers.
- Server Configuration Changes: Logs changes to server settings, roles, and channels.
- Advanced Database Integration: Utilizes PostgreSQL for efficient and scalable data storage.
- Slash Command Interface: Provides easy access to server statistics and Spotify listening trends.

## Commands

- /stats: Displays comprehensive server statistics, optionally within a specified date range.
- /spotify_top: Shows top Spotify songs, artists, or albums listened to in the server, with customizable limits and date ranges.

## Technical Details

- Language: Python 3.8+
- Framework: discord.py
- Database: PostgreSQL
- Key Dependencies: asyncpg, pytz

## Setup

1. Clone the repository
2. Install required dependencies:
   pip install discord.py asyncpg pytz
3. Set up a PostgreSQL database
4. Configure the database connection in the script:
   user='your_user', password='your_password', database='your_db', host='localhost'
5. Replace the bot token in the bot.start() call with your own Discord bot token
6. Run the script:
   python bot_script.py

## Database Schema

The bot uses a complex database schema with tables for various types of events and activities. Key tables include:

- messages
- message_edits
- message_deletions
- voice_activity
- role_changes
- moderation_actions
- spotify_activity
- (and many more)

Each table is indexed for efficient querying.

## Error Handling and Logging

The bot implements comprehensive error handling and logging:

- Errors are caught, logged, and reported without crashing the bot
- A rotating file handler is used to manage log files
- Detailed tracebacks are recorded for debugging

## Scalability and Performance

- Utilizes connection pooling for database operations
- Implements a queue system for batch inserts to reduce database load
- Uses efficient asynchronous programming practices

## Disclaimer

This bot collects and stores a significant amount of user data. If implemented, ensure compliance with Discord's Terms of Service and relevant data protection laws. Inform your server members about the data being collected and stored and obtain their explicit consent. The authors and contributors of this project do not endorse or encourage the use of this bot for data collection purposes without proper user consent and privacy safeguards.