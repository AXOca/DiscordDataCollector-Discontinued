import asyncio
import discord
from discord.ext import commands, tasks
from discord import app_commands
from discord import app_commands
import asyncpg
from datetime import datetime, timezone
import logging
from logging.handlers import RotatingFileHandler
import traceback
from collections import deque
import pytz
import time
import asyncio
import signal
import sys
import aiohttp

# Enhanced logging setup
german_timezone = pytz.timezone('Europe/Berlin')
logger = logging.getLogger('discord_bot')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('discord_bot.log', maxBytes=10000000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Database connection pool
async def create_db_pool():
    try:
        return await asyncpg.create_pool(
            user='your_user', password='your_password',
            database='your_db', host='localhost',
            min_size=5, max_size=20  # Connection pooling
        )
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}\n{traceback.format_exc()}")
        return None

# Bot setup
intents = discord.Intents.all()
bot = commands.Bot(command_prefix='!', intents=intents)

# Queue for batch inserts
event_queue = deque(maxlen=1000)

# Function to get current Unix timestamp
def get_current_timestamp():
    return int(time.time())

# Database setup
async def setup_database(pool):
    if pool is None:
        logger.error("Database pool is None. Cannot set up database.")
        return

    async with pool.acquire() as conn:
        try:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    user_id BIGINT,
                    channel_id BIGINT,
                    content TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS message_edits (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    user_id BIGINT,
                    channel_id BIGINT,
                    content TEXT,
                    edit_timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS message_deletions (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    user_id BIGINT,
                    channel_id BIGINT,
                    content TEXT,
                    deletion_timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS threads (
                    id SERIAL PRIMARY KEY,
                    thread_id BIGINT,
                    name TEXT,
                    creator_id BIGINT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS thread_deletions (
                    id SERIAL PRIMARY KEY,
                    thread_id BIGINT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS forum_channels (
                    id SERIAL PRIMARY KEY,
                    channel_id BIGINT,
                    name TEXT,
                    creator_id BIGINT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS forum_channel_deletions (
                    id SERIAL PRIMARY KEY,
                    channel_id BIGINT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS moderation_actions (
                    id SERIAL PRIMARY KEY,
                    action_type TEXT,
                    user_id BIGINT,
                    moderator_id BIGINT,
                    reason TEXT,
                    duration INTEGER,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS voice_activity (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    channel_id BIGINT,
                    action TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS role_changes (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    role_id BIGINT,
                    change_type TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS nickname_changes (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    old_nick TEXT,
                    new_nick TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS reactions (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    user_id BIGINT,
                    emoji TEXT,
                    action TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS member_events (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    event_type TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS message_attachments (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    file_name TEXT,
                    file_size INTEGER,
                    file_url TEXT,
                    content_type TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS server_changes (
                    id SERIAL PRIMARY KEY,
                    change_type TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS invites (
                    id SERIAL PRIMARY KEY,
                    invite_code TEXT,
                    creator_id BIGINT,
                    channel_id BIGINT,
                    max_uses INTEGER,
                    uses INTEGER,
                    created_at BIGINT,
                    expires_at BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS webhook_messages (
                    id SERIAL PRIMARY KEY,
                    webhook_id BIGINT,
                    channel_id BIGINT,
                    content TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS slash_command_uses (
                    id SERIAL PRIMARY KEY,
                    command_name TEXT,
                    user_id BIGINT,
                    channel_id BIGINT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_profile_changes (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    change_type TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    timestamp BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS channel_updates (
                    id SERIAL PRIMARY KEY,
                    channel_id BIGINT,
                    change_type TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS role_updates (
                    id SERIAL PRIMARY KEY,
                    role_id BIGINT,
                    change_type TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bulk_message_deletions (
                    id SERIAL PRIMARY KEY,
                    channel_id BIGINT,
                    message_count INTEGER,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bot_guild_events (
                    id SERIAL PRIMARY KEY,
                    event_type TEXT,
                    guild_id BIGINT,
                    timestamp BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS thread_events (
                    id SERIAL PRIMARY KEY,
                    thread_id BIGINT,
                    event_type TEXT,
                    user_id BIGINT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS stage_events (
                    id SERIAL PRIMARY KEY,
                    channel_id BIGINT,
                    event_type TEXT,
                    user_id BIGINT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS pinned_messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    channel_id BIGINT,
                    user_id BIGINT,
                    action TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS server_boosts (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    action TEXT,
                    tier INTEGER,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS emoji_sticker_changes (
                    id SERIAL PRIMARY KEY,
                    item_id BIGINT,
                    item_type TEXT,
                    action TEXT,
                    name TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS integration_changes (
                    id SERIAL PRIMARY KEY,
                    integration_id BIGINT,
                    integration_type TEXT,
                    action TEXT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS channel_permission_changes (
                    id SERIAL PRIMARY KEY,
                    channel_id BIGINT,
                    target_id BIGINT,
                    target_type TEXT,
                    allow BIGINT,
                    deny BIGINT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS widget_changes (
                    id SERIAL PRIMARY KEY,
                    enabled BOOLEAN,
                    channel_id BIGINT,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS role_mentionability (
                    id SERIAL PRIMARY KEY,
                    role_id BIGINT,
                    mentionable BOOLEAN,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS slowmode_changes (
                    id SERIAL PRIMARY KEY,
                    channel_id BIGINT,
                    slowmode_delay INTEGER,
                    timestamp BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_activity (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    activity_type TEXT,
                    activity_name TEXT,
                    start_time BIGINT,
                    end_time BIGINT,
                    guild_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS spotify_activity (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    guild_id BIGINT,
                    song_title TEXT,
                    artist TEXT,
                    album TEXT,
                    start_time BIGINT,
                    end_time BIGINT,
                    duration INTEGER
                )
            ''')

            # Create indexes
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_guild_id ON messages(guild_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_message_edits_message_id ON message_edits(message_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_message_deletions_message_id ON message_deletions(message_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_threads_thread_id ON threads(thread_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_thread_deletions_thread_id ON thread_deletions(thread_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_forum_channels_channel_id ON forum_channels(channel_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_forum_channel_deletions_channel_id ON forum_channel_deletions(channel_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_moderation_actions_user_id ON moderation_actions(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_voice_activity_user_id ON voice_activity(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_role_changes_user_id ON role_changes(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_nickname_changes_user_id ON nickname_changes(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_reactions_message_id ON reactions(message_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_member_events_user_id ON member_events(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_message_attachments_message_id ON message_attachments(message_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_server_changes_guild_id ON server_changes(guild_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_invites_guild_id ON invites(guild_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_webhook_messages_guild_id ON webhook_messages(guild_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_slash_command_uses_guild_id ON slash_command_uses(guild_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_profile_changes_user_id ON user_profile_changes(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_channel_updates_channel_id ON channel_updates(channel_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_role_updates_role_id ON role_updates(role_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_bulk_message_deletions_guild_id ON bulk_message_deletions(guild_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_bot_guild_events_guild_id ON bot_guild_events(guild_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_thread_events_thread_id ON thread_events(thread_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_stage_events_channel_id ON stage_events(channel_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_pinned_messages_message_id ON pinned_messages(message_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_server_boosts_user_id ON server_boosts(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_emoji_sticker_changes_item_id ON emoji_sticker_changes(item_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_integration_changes_integration_id ON integration_changes(integration_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_channel_permission_changes_channel_id ON channel_permission_changes(channel_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_widget_changes_guild_id ON widget_changes(guild_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_role_mentionability_role_id ON role_mentionability(role_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_slowmode_changes_channel_id ON slowmode_changes(channel_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_activity_user_id ON user_activity(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_spotify_activity_user_id ON spotify_activity(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_spotify_activity_guild_id ON spotify_activity(guild_id)')

        except Exception as e:
            logger.error(f"Error setting up database: {e}\n{traceback.format_exc()}")

def validate_data(data):
    return tuple(int(item.timestamp()) if isinstance(item, datetime) else item for item in data)

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    bot.db_pool = await create_db_pool()
    if bot.db_pool:
        await setup_database(bot.db_pool)
        process_queue.start()
        await sync_commands()
    else:
        logger.error("Failed to start the bot due to database connection issues.")

@tasks.loop(seconds=5.0)
async def process_queue():
    if not event_queue:
        return

    async with bot.db_pool.acquire() as conn:
        async with conn.transaction():
            while event_queue:
                query, values = event_queue.popleft()
                try:
                    await conn.execute(query, *values)
                except Exception as e:
                    logger.error(f"Error processing queue item: {e}\n{traceback.format_exc()}")
                    logger.error(f"Problematic query: {query}")
                    logger.error(f"Problematic values: {values}")

@bot.event
async def on_message(message):
    try:
        content = message.content
        if not content and message.attachments:
            content = ' '.join([attachment.url for attachment in message.attachments])

        timestamp = get_current_timestamp()
        data = validate_data((message.id, message.author.id, message.channel.id, content, timestamp, message.guild.id))
        event_queue.append((
            '''INSERT INTO messages (message_id, user_id, channel_id, content, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6)''',
            data
        ))

        # Log attachments
        for attachment in message.attachments:
            attachment_data = validate_data((
                message.id, attachment.filename, attachment.size, 
                attachment.url, attachment.content_type, timestamp, message.guild.id
            ))
            event_queue.append((
                '''INSERT INTO message_attachments (message_id, file_name, file_size, file_url, content_type, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)''',
                attachment_data
            ))

        # Log webhook messages
        if message.webhook_id:
            webhook_data = validate_data((message.webhook_id, message.channel.id, content, timestamp, message.guild.id))
            event_queue.append((
                '''INSERT INTO webhook_messages (webhook_id, channel_id, content, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                webhook_data
            ))

        await bot.process_commands(message)
    except Exception as e:
        logger.error(f"Error in on_message: {e}\n{traceback.format_exc()}")

@bot.event
async def on_message_edit(before, after):
    try:
        if before.content != after.content:
            content = after.content
            if not content and after.attachments:
                content = ' '.join([attachment.url for attachment in after.attachments])

            timestamp = get_current_timestamp()
            data = validate_data((after.id, after.author.id, after.channel.id, content, timestamp, after.guild.id))
            event_queue.append((
                '''INSERT INTO message_edits (message_id, user_id, channel_id, content, edit_timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5, $6)''',
                data
            ))
    except Exception as e:
        logger.error(f"Error in on_message_edit: {e}\n{traceback.format_exc()}")

@bot.event
async def on_message_delete(message):
    try:
        content = message.content
        if not content and message.attachments:
            content = ' '.join([attachment.url for attachment in message.attachments])

        timestamp = get_current_timestamp()
        data = validate_data((message.id, message.author.id, message.channel.id, content, timestamp, message.guild.id))
        event_queue.append((
            '''INSERT INTO message_deletions (message_id, user_id, channel_id, content, deletion_timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_message_delete: {e}\n{traceback.format_exc()}")

@bot.event
async def on_bulk_message_delete(messages):
    try:
        if messages:
            channel_id = messages[0].channel.id
            guild_id = messages[0].guild.id
            timestamp = get_current_timestamp()
            data = validate_data((channel_id, len(messages), timestamp, guild_id))
            event_queue.append((
                '''INSERT INTO bulk_message_deletions (channel_id, message_count, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4)''',
                data
            ))
            # Log individual message details
            for message in messages:
                content = message.content if message.content else ' '.join([attachment.url for attachment in message.attachments])
                message_data = validate_data((message.id, message.author.id, channel_id, content, timestamp, guild_id))
                event_queue.append((
                    '''INSERT INTO message_deletions (message_id, user_id, channel_id, content, deletion_timestamp, guild_id)
                       VALUES ($1, $2, $3, $4, $5, $6)''',
                    message_data
                ))
    except Exception as e:
        logger.error(f"Error in on_bulk_message_delete: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_channel_create(channel):
    try:
        timestamp = get_current_timestamp()
        if isinstance(channel, discord.ForumChannel):
            data = validate_data((channel.id, channel.name, channel.guild.owner_id, timestamp, channel.guild.id))
            event_queue.append((
                '''INSERT INTO forum_channels (channel_id, name, creator_id, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
        elif isinstance(channel, discord.StageChannel):
            data = validate_data((channel.id, 'create', channel.guild.me.id, timestamp, channel.guild.id))
            event_queue.append((
                '''INSERT INTO stage_events (channel_id, event_type, user_id, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
    except Exception as e:
        logger.error(f"Error in on_guild_channel_create: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_channel_delete(channel):
    try:
        timestamp = get_current_timestamp()
        if isinstance(channel, discord.ForumChannel):
            data = validate_data((channel.id, timestamp, channel.guild.id))
            event_queue.append((
                '''INSERT INTO forum_channel_deletions (channel_id, timestamp, guild_id)
                   VALUES ($1, $2, $3)''',
                data
            ))
        elif isinstance(channel, discord.StageChannel):
            data = validate_data((channel.id, 'delete', channel.guild.me.id, timestamp, channel.guild.id))
            event_queue.append((
                '''INSERT INTO stage_events (channel_id, event_type, user_id, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
    except Exception as e:
        logger.error(f"Error in on_guild_channel_delete: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_channel_update(before, after):
    try:
        timestamp = get_current_timestamp()
        changes = []
        if before.name != after.name:
            changes.append(('name', before.name, after.name))
        if isinstance(before, discord.TextChannel):
            if before.topic != after.topic:
                changes.append(('topic', before.topic, after.topic))
            if before.slowmode_delay != after.slowmode_delay:
                changes.append(('slowmode_delay', str(before.slowmode_delay), str(after.slowmode_delay)))
                # Log slowmode change
                slowmode_data = validate_data((after.id, after.slowmode_delay, timestamp, after.guild.id))
                event_queue.append((
                    '''INSERT INTO slowmode_changes (channel_id, slowmode_delay, timestamp, guild_id)
                       VALUES ($1, $2, $3, $4)''',
                    slowmode_data
                ))

        for change_type, old_value, new_value in changes:
            data = validate_data((after.id, change_type, old_value, new_value, timestamp, after.guild.id))
            event_queue.append((
                '''INSERT INTO channel_updates (channel_id, change_type, old_value, new_value, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5, $6)''',
                data
            ))

        # Check for permission overwrite changes
        for target, overwrite in after.overwrites.items():
            if before.overwrites.get(target) != overwrite:
                target_type = 'role' if isinstance(target, discord.Role) else 'member'
                data = validate_data((after.id, target.id, target_type, overwrite.pair()[0].value, overwrite.pair()[1].value, timestamp, after.guild.id))
                event_queue.append((
                    '''INSERT INTO channel_permission_changes (channel_id, target_id, target_type, allow, deny, timestamp, guild_id)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)''',
                    data
                ))

    except Exception as e:
        logger.error(f"Error in on_guild_channel_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_role_create(role):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((role.id, 'create', None, role.name, timestamp, role.guild.id))
        event_queue.append((
            '''INSERT INTO role_updates (role_id, change_type, old_value, new_value, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_role_create: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_role_delete(role):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((role.id, 'delete', role.name, None, timestamp, role.guild.id))
        event_queue.append((
            '''INSERT INTO role_updates (role_id, change_type, old_value, new_value, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_role_delete: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_role_update(before, after):
    try:
        timestamp = get_current_timestamp()
        changes = []
        if before.name != after.name:
            changes.append(('name', before.name, after.name))
        if before.permissions != after.permissions:
            changes.append(('permissions', str(before.permissions.value), str(after.permissions.value)))
        if before.color != after.color:
            changes.append(('color', str(before.color), str(after.color)))
        if before.mentionable != after.mentionable:
            changes.append(('mentionable', str(before.mentionable), str(after.mentionable)))
            # Log role mentionability change
            mentionable_data = validate_data((after.id, after.mentionable, timestamp, after.guild.id))
            event_queue.append((
                '''INSERT INTO role_mentionability (role_id, mentionable, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4)''',
                mentionable_data
            ))

        for change_type, old_value, new_value in changes:
            data = validate_data((after.id, change_type, old_value, new_value, timestamp, after.guild.id))
            event_queue.append((
                '''INSERT INTO role_updates (role_id, change_type, old_value, new_value, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5, $6)''',
                data
            ))
    except Exception as e:
        logger.error(f"Error in on_guild_role_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_emojis_update(guild, before, after):
    try:
        timestamp = get_current_timestamp()
        # Check for added emojis
        for emoji in set(after) - set(before):
            data = validate_data((emoji.id, 'emoji', 'create', emoji.name, timestamp, guild.id))
            event_queue.append((
                '''INSERT INTO emoji_sticker_changes (item_id, item_type, action, name, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5, $6)''',
                data
            ))

        # Check for removed emojis
        for emoji in set(before) - set(after):
            data = validate_data((emoji.id, 'emoji', 'delete', emoji.name, timestamp, guild.id))
            event_queue.append((
                '''INSERT INTO emoji_sticker_changes (item_id, item_type, action, name, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5, $6)''',
                data
            ))

        # Check for updated emojis
        for before_emoji in before:
            after_emoji = discord.utils.get(after, id=before_emoji.id)
            if after_emoji and before_emoji.name != after_emoji.name:
                data = validate_data((after_emoji.id, 'emoji', 'update', after_emoji.name, timestamp, guild.id))
                event_queue.append((
                    '''INSERT INTO emoji_sticker_changes (item_id, item_type, action, name, timestamp, guild_id)
                       VALUES ($1, $2, $3, $4, $5, $6)''',
                    data
                ))
    except Exception as e:
        logger.error(f"Error in on_guild_emojis_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_stickers_update(guild, before, after):
    try:
        timestamp = get_current_timestamp()
        # Check for added stickers
        for sticker in set(after) - set(before):
            data = validate_data((sticker.id, 'sticker', 'create', sticker.name, timestamp, guild.id))
            event_queue.append((
                '''INSERT INTO emoji_sticker_changes (item_id, item_type, action, name, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5, $6)''',
                data
            ))

        # Check for removed stickers
        for sticker in set(before) - set(after):
            data = validate_data((sticker.id, 'sticker', 'delete', sticker.name, timestamp, guild.id))
            event_queue.append((
                '''INSERT INTO emoji_sticker_changes (item_id, item_type, action, name, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5, $6)''',
                data
            ))

        # Check for updated stickers
        for before_sticker in before:
            after_sticker = discord.utils.get(after, id=before_sticker.id)
            if after_sticker and before_sticker.name != after_sticker.name:
                data = validate_data((after_sticker.id, 'sticker', 'update', after_sticker.name, timestamp, guild.id))
                event_queue.append((
                    '''INSERT INTO emoji_sticker_changes (item_id, item_type, action, name, timestamp, guild_id)
                       VALUES ($1, $2, $3, $4, $5, $6)''',
                    data
                ))
    except Exception as e:
        logger.error(f"Error in on_guild_stickers_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_update(before, after):
    try:
        timestamp = get_current_timestamp()
        changes = []
        if before.name != after.name:
            changes.append(('name', before.name, after.name))
        if before.icon != after.icon:
            changes.append(('icon', str(before.icon), str(after.icon)))
        if before.region != after.region:
            changes.append(('region', str(before.region), str(after.region)))
        if before.verification_level != after.verification_level:
            changes.append(('verification_level', str(before.verification_level), str(after.verification_level)))
        if before.default_notifications != after.default_notifications:
            changes.append(('default_notifications', str(before.default_notifications), str(after.default_notifications)))
        if before.explicit_content_filter != after.explicit_content_filter:
            changes.append(('explicit_content_filter', str(before.explicit_content_filter), str(after.explicit_content_filter)))
        if before.afk_channel != after.afk_channel:
            changes.append(('afk_channel', str(before.afk_channel), str(after.afk_channel)))
        if before.afk_timeout != after.afk_timeout:
            changes.append(('afk_timeout', str(before.afk_timeout), str(after.afk_timeout)))
        if before.mfa_level != after.mfa_level:
            changes.append(('mfa_level', str(before.mfa_level), str(after.mfa_level)))
        if before.widget_enabled != after.widget_enabled or before.widget_channel != after.widget_channel:
            changes.append(('widget', f"{before.widget_enabled}:{before.widget_channel}", f"{after.widget_enabled}:{after.widget_channel}"))
            # Log widget changes
            widget_data = validate_data((after.widget_enabled, after.widget_channel.id if after.widget_channel else None, timestamp, after.id))
            event_queue.append((
                '''INSERT INTO widget_changes (enabled, channel_id, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4)''',
                widget_data
            ))

        for change_type, old_value, new_value in changes:
            data = validate_data((change_type, old_value, new_value, timestamp, after.id))
            event_queue.append((
                '''INSERT INTO server_changes (change_type, old_value, new_value, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
    except Exception as e:
        logger.error(f"Error in on_guild_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_integrations_update(guild):
    try:
        timestamp = get_current_timestamp()
        # Note: This event doesn't provide details about the change, so we just log that an update occurred
        data = validate_data((0, 'unknown', 'update', timestamp, guild.id))
        event_queue.append((
            '''INSERT INTO integration_changes (integration_id, integration_type, action, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_integrations_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_webhooks_update(channel):
    try:
        timestamp = get_current_timestamp()
        # Note: This event doesn't provide details about the change, so we just log that an update occurred
        data = validate_data((0, 'webhook', 'update', timestamp, channel.guild.id))
        event_queue.append((
            '''INSERT INTO integration_changes (integration_id, integration_type, action, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_webhooks_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_member_join(member):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((member.id, 'join', timestamp, member.guild.id))
        event_queue.append((
            '''INSERT INTO member_events (user_id, event_type, timestamp, guild_id)
               VALUES ($1, $2, $3, $4)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_member_join: {e}\n{traceback.format_exc()}")

@bot.event
async def on_member_remove(member):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((member.id, 'leave', timestamp, member.guild.id))
        event_queue.append((
            '''INSERT INTO member_events (user_id, event_type, timestamp, guild_id)
               VALUES ($1, $2, $3, $4)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_member_remove: {e}\n{traceback.format_exc()}")

@bot.event
async def on_member_update(before, after):
    try:
        timestamp = get_current_timestamp()
        # Check for nickname changes
        if before.nick != after.nick:
            data = validate_data((after.id, before.nick, after.nick, timestamp, after.guild.id))
            event_queue.append((
                '''INSERT INTO nickname_changes (user_id, old_nick, new_nick, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
        
        # Check for role changes
        added_roles = set(after.roles) - set(before.roles)
        removed_roles = set(before.roles) - set(after.roles)
        
        for role in added_roles:
            data = validate_data((after.id, role.id, 'add', timestamp, after.guild.id))
            event_queue.append((
                '''INSERT INTO role_changes (user_id, role_id, change_type, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
        
        for role in removed_roles:
            data = validate_data((after.id, role.id, 'remove', timestamp, after.guild.id))
            event_queue.append((
                '''INSERT INTO role_changes (user_id, role_id, change_type, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))

        # Check for timeout changes
        if before.timed_out_until != after.timed_out_until:
            if after.timed_out_until is not None:
                duration = int((after.timed_out_until.replace(tzinfo=timezone.utc) - datetime.now(timezone.utc)).total_seconds())
                data = validate_data(('timeout', after.id, None, None, duration, timestamp, after.guild.id))
                event_queue.append((
                    '''INSERT INTO moderation_actions (action_type, user_id, moderator_id, reason, duration, timestamp, guild_id)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)''',
                    data
                ))
            else:
                data = validate_data(('untimeout', after.id, None, None, None, timestamp, after.guild.id))
                event_queue.append((
                    '''INSERT INTO moderation_actions (action_type, user_id, moderator_id, reason, duration, timestamp, guild_id)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)''',
                    data
                ))
    except Exception as e:
        logger.error(f"Error in on_member_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_user_update(before, after):
    try:
        timestamp = get_current_timestamp()
        if before.name != after.name:
            data = validate_data((after.id, 'username', before.name, after.name, timestamp))
            event_queue.append((
                '''INSERT INTO user_profile_changes (user_id, change_type, old_value, new_value, timestamp)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
        if before.discriminator != after.discriminator:
            data = validate_data((after.id, 'discriminator', before.discriminator, after.discriminator, timestamp))
            event_queue.append((
                '''INSERT INTO user_profile_changes (user_id, change_type, old_value, new_value, timestamp)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
        if before.avatar != after.avatar:
            data = validate_data((after.id, 'avatar', str(before.avatar), str(after.avatar), timestamp))
            event_queue.append((
                '''INSERT INTO user_profile_changes (user_id, change_type, old_value, new_value, timestamp)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
    except Exception as e:
        logger.error(f"Error in on_user_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_voice_state_update(member, before, after):
    try:
        timestamp = get_current_timestamp()
        if before.channel != after.channel:
            if after.channel:
                action = 'join'
                channel_id = after.channel.id
            else:
                action = 'leave'
                channel_id = before.channel.id if before.channel else None

            data = validate_data((member.id, channel_id, action, timestamp, member.guild.id))
            event_queue.append((
                '''INSERT INTO voice_activity (user_id, channel_id, action, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
        
        # Log mute/deafen changes
        if before.self_mute != after.self_mute:
            action = 'mute' if after.self_mute else 'unmute'
            data = validate_data((member.id, after.channel.id if after.channel else None, action, timestamp, member.guild.id))
            event_queue.append((
                '''INSERT INTO voice_activity (user_id, channel_id, action, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
        
        if before.self_deaf != after.self_deaf:
            action = 'deafen' if after.self_deaf else 'undeafen'
            data = validate_data((member.id, after.channel.id if after.channel else None, action, timestamp, member.guild.id))
            event_queue.append((
                '''INSERT INTO voice_activity (user_id, channel_id, action, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
        
        # Log streaming start/stop
        if before.self_stream != after.self_stream:
            action = 'start_stream' if after.self_stream else 'stop_stream'
            data = validate_data((member.id, after.channel.id if after.channel else None, action, timestamp, member.guild.id))
            event_queue.append((
                '''INSERT INTO voice_activity (user_id, channel_id, action, timestamp, guild_id)
                   VALUES ($1, $2, $3, $4, $5)''',
                data
            ))
    except Exception as e:
        logger.error(f"Error in on_voice_state_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_channel_pins_update(channel, last_pin):
    try:
        timestamp = get_current_timestamp()
        # Note: This event doesn't provide information about which message was pinned/unpinned
        action = 'pin' if last_pin else 'unpin'
        data = validate_data((0, channel.id, 0, action, timestamp, channel.guild.id))
        event_queue.append((
            '''INSERT INTO pinned_messages (message_id, channel_id, user_id, action, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_channel_pins_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_available(guild):
    try:
        timestamp = get_current_timestamp()
        data = validate_data(('available', guild.id, timestamp))
        event_queue.append((
            '''INSERT INTO bot_guild_events (event_type, guild_id, timestamp)
               VALUES ($1, $2, $3)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_available: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_unavailable(guild):
    try:
        timestamp = get_current_timestamp()
        data = validate_data(('unavailable', guild.id, timestamp))
        event_queue.append((
            '''INSERT INTO bot_guild_events (event_type, guild_id, timestamp)
               VALUES ($1, $2, $3)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_unavailable: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_join(guild):
    try:
        timestamp = get_current_timestamp()
        data = validate_data(('join', guild.id, timestamp))
        event_queue.append((
            '''INSERT INTO bot_guild_events (event_type, guild_id, timestamp)
               VALUES ($1, $2, $3)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_join: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_remove(guild):
    try:
        timestamp = get_current_timestamp()
        data = validate_data(('leave', guild.id, timestamp))
        event_queue.append((
            '''INSERT INTO bot_guild_events (event_type, guild_id, timestamp)
               VALUES ($1, $2, $3)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_remove: {e}\n{traceback.format_exc()}")

@bot.event
async def on_member_ban(guild, user):
    try:
        timestamp = get_current_timestamp()
        ban_entry = await guild.fetch_ban(user)
        data = validate_data(('ban', user.id, None, ban_entry.reason, None, timestamp, guild.id))
        event_queue.append((
            '''INSERT INTO moderation_actions (action_type, user_id, moderator_id, reason, duration, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_member_ban: {e}\n{traceback.format_exc()}")

@bot.event
async def on_member_unban(guild, user):
    try:
        timestamp = get_current_timestamp()
        data = validate_data(('unban', user.id, None, None, None, timestamp, guild.id))
        event_queue.append((
            '''INSERT INTO moderation_actions (action_type, user_id, moderator_id, reason, duration, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_member_unban: {e}\n{traceback.format_exc()}")

@bot.event
async def on_invite_create(invite):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((
            invite.code, invite.inviter.id if invite.inviter else None,
            invite.channel.id, invite.max_uses, 0, int(invite.created_at.timestamp()),
            int(invite.expires_at.timestamp()) if invite.expires_at else None, invite.guild.id
        ))
        event_queue.append((
            '''INSERT INTO invites (invite_code, creator_id, channel_id, max_uses, uses, created_at, expires_at, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_invite_create: {e}\n{traceback.format_exc()}")

@bot.event
async def on_invite_delete(invite):
    try:
        timestamp = get_current_timestamp()
        # Note: The invite object might not have all information available when deleted
        data = validate_data((invite.code, invite.guild.id if invite.guild else None, timestamp))
        event_queue.append((
            '''UPDATE invites SET uses = max_uses, expires_at = $3
               WHERE invite_code = $1 AND guild_id = $2''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_invite_delete: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_scheduled_event_create(event):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((event.id, 'create', event.name, event.description, int(event.start_time.timestamp()), int(event.end_time.timestamp()) if event.end_time else None, event.guild.id))
        event_queue.append((
            '''INSERT INTO scheduled_events (event_id, action, name, description, start_time, end_time, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_scheduled_event_create: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_scheduled_event_update(before, after):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((after.id, 'update', after.name, after.description, int(after.start_time.timestamp()), int(after.end_time.timestamp()) if after.end_time else None, after.guild.id))
        event_queue.append((
            '''INSERT INTO scheduled_events (event_id, action, name, description, start_time, end_time, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_scheduled_event_update: {e}\n{traceback.format_exc()}")

@bot.event
async def on_guild_scheduled_event_delete(event):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((event.id, 'delete', event.name, event.description, int(event.start_time.timestamp()), int(event.end_time.timestamp()) if event.end_time else None, event.guild.id))
        event_queue.append((
            '''INSERT INTO scheduled_events (event_id, action, name, description, start_time, end_time, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_guild_scheduled_event_delete: {e}\n{traceback.format_exc()}")

@bot.event
async def on_reaction_add(reaction, user):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((reaction.message.id, user.id, str(reaction.emoji), 'add', timestamp, reaction.message.guild.id))
        event_queue.append((
            '''INSERT INTO reactions (message_id, user_id, emoji, action, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_reaction_add: {e}\n{traceback.format_exc()}")

@bot.event
async def on_reaction_remove(reaction, user):
    try:
        timestamp = get_current_timestamp()
        data = validate_data((reaction.message.id, user.id, str(reaction.emoji), 'remove', timestamp, reaction.message.guild.id))
        event_queue.append((
            '''INSERT INTO reactions (message_id, user_id, emoji, action, timestamp, guild_id)
               VALUES ($1, $2, $3, $4, $5, $6)''',
            data
        ))
    except Exception as e:
        logger.error(f"Error in on_reaction_remove: {e}\n{traceback.format_exc()}")

@bot.event
async def on_presence_update(before, after):
    try:
        timestamp = get_current_timestamp()
        
        # Check for Spotify activity changes
        before_spotify = discord.utils.find(lambda a: isinstance(a, discord.Spotify), before.activities)
        after_spotify = discord.utils.find(lambda a: isinstance(a, discord.Spotify), after.activities)

        if before_spotify != after_spotify:
            if after_spotify:
                # Spotify activity started or changed
                start_time = int(after_spotify.start.timestamp())
                duration = int(after_spotify.duration.total_seconds())
                data = validate_data((
                    after.id, 
                    after.guild.id,
                    after_spotify.title,
                    after_spotify.artist,
                    after_spotify.album,
                    start_time,
                    None,  # end_time is initially None
                    duration
                ))
                event_queue.append((
                    '''INSERT INTO spotify_activity 
                       (user_id, guild_id, song_title, artist, album, start_time, end_time, duration)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)''',
                    data
                ))
            elif before_spotify:
                # Spotify activity ended
                end_time = timestamp
                data = validate_data((end_time, before.id, before.guild.id))
                event_queue.append((
                    '''UPDATE spotify_activity 
                       SET end_time = $1
                       WHERE user_id = $2 AND guild_id = $3 AND end_time IS NULL''',
                    data
                ))

        # Other activity tracking remains the same
        if before.activities != after.activities:
            for activity in set(after.activities) - set(before.activities):
                data = validate_data((after.id, str(activity.type), activity.name, timestamp, after.guild.id))
                event_queue.append((
                    '''INSERT INTO user_activity (user_id, activity_type, activity_name, start_time, guild_id)
                       VALUES ($1, $2, $3, $4, $5)''',
                    data
                ))
            for activity in set(before.activities) - set(after.activities):
                data = validate_data((before.id, str(activity.type), activity.name, timestamp, before.guild.id))
                event_queue.append((
                    '''UPDATE user_activity SET end_time = $4
                       WHERE user_id = $1 AND activity_type = $2 AND activity_name = $3 AND guild_id = $5 AND end_time IS NULL''',
                    data
                ))
    except Exception as e:
        logger.error(f"Error in on_presence_update: {e}\n{traceback.format_exc()}")

shutdown_event = asyncio.Event()

async def close_aiohttp_sessions():
    for task in asyncio.all_tasks():
        if isinstance(task._coro, aiohttp.ClientSession.close):
            await task

async def shutdown(loop):
    if shutdown_event.is_set():
        return
    shutdown_event.set()
    
    print("Initiating shutdown...")
    
    # Close the Discord client connection
    print("Closing Discord client connection...")
    await bot.close()
    
    # Close aiohttp sessions
    print("Closing aiohttp sessions...")
    await close_aiohttp_sessions()
    
    # Cancel all tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    print(f"Cancelling {len(tasks)} outstanding tasks")
    
    # Wait for all tasks to complete with a timeout
    try:
        await asyncio.wait(tasks, timeout=5)
    except asyncio.TimeoutError:
        print("Timeout waiting for tasks to complete. Some tasks may not have finished.")
    
    # Close the database pool
    print("Shutting down database connection pool...")
    if hasattr(bot, 'db_pool'):
        await bot.db_pool.close()
    
    # Close any remaining connections
    await asyncio.sleep(1)  # Give a moment for connections to close
    for task in asyncio.all_tasks():
        if not task.done():
            task.cancel()
    
    # Stop the event loop
    loop.stop()

def signal_handler(sig, frame):
    print(f"Received exit signal {signal.Signals(sig).name}...")
    loop = asyncio.get_event_loop()
    loop.create_task(shutdown(loop))

def handle_exception(loop, context):
    # context["message"] will always be there; but context["exception"] may not
    msg = context.get("exception", context["message"])
    print(f"Caught exception: {msg}")
    print("Shutting down...")
    asyncio.create_task(shutdown(signal.SIGINT, loop))

async def sync_commands():
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} command(s)")
    except Exception as e:
        logger.error(f"Failed to sync commands: {e}\n{traceback.format_exc()}")

@bot.tree.command(name="stats", description="Display server statistics")
@app_commands.describe(
    start_date="Start date for stats (YYYY-MM-DD, optional)",
    end_date="End date for stats (YYYY-MM-DD, optional)"
)
@app_commands.guild_only()
async def stats(interaction: discord.Interaction, start_date: str = None, end_date: str = None):
    try:
        async with bot.db_pool.acquire() as conn:
            # Convert dates to timestamps if provided
            start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp()) if start_date else None
            end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp()) if end_date else None

            # Function to create the date condition for SQL queries
            def date_condition(column_name):
                if start_timestamp and end_timestamp:
                    return f"AND {column_name} BETWEEN {start_timestamp} AND {end_timestamp}"
                elif start_timestamp:
                    return f"AND {column_name} >= {start_timestamp}"
                elif end_timestamp:
                    return f"AND {column_name} <= {end_timestamp}"
                else:
                    return ""

            # Modify queries to include date range if specified
            message_count = await conn.fetchval(f'SELECT COUNT(*) FROM messages WHERE guild_id = $1 {date_condition("timestamp")}', interaction.guild_id)
            thread_count = await conn.fetchval(f'SELECT COUNT(*) FROM threads WHERE guild_id = $1 {date_condition("timestamp")}', interaction.guild_id)
            ban_count = await conn.fetchval(f'SELECT COUNT(*) FROM moderation_actions WHERE guild_id = $1 AND action_type = $2 {date_condition("timestamp")}', interaction.guild_id, 'ban')
            edit_count = await conn.fetchval(f'SELECT COUNT(*) FROM message_edits WHERE guild_id = $1 {date_condition("edit_timestamp")}', interaction.guild_id)
            deletion_count = await conn.fetchval(f'SELECT COUNT(*) FROM message_deletions WHERE guild_id = $1 {date_condition("deletion_timestamp")}', interaction.guild_id)
            role_change_count = await conn.fetchval(f'SELECT COUNT(*) FROM role_changes WHERE guild_id = $1 {date_condition("timestamp")}', interaction.guild_id)
            nickname_change_count = await conn.fetchval(f'SELECT COUNT(*) FROM nickname_changes WHERE guild_id = $1 {date_condition("timestamp")}', interaction.guild_id)
            reaction_count = await conn.fetchval(f'SELECT COUNT(*) FROM reactions WHERE guild_id = $1 {date_condition("timestamp")}', interaction.guild_id)
            member_join_count = await conn.fetchval(f'SELECT COUNT(*) FROM member_events WHERE guild_id = $1 AND event_type = $2 {date_condition("timestamp")}', interaction.guild_id, 'join')
            member_leave_count = await conn.fetchval(f'SELECT COUNT(*) FROM member_events WHERE guild_id = $1 AND event_type = $2 {date_condition("timestamp")}', interaction.guild_id, 'leave')
            emoji_sticker_change_count = await conn.fetchval(f'SELECT COUNT(*) FROM emoji_sticker_changes WHERE guild_id = $1 {date_condition("timestamp")}', interaction.guild_id)
            channel_permission_change_count = await conn.fetchval(f'SELECT COUNT(*) FROM channel_permission_changes WHERE guild_id = $1 {date_condition("timestamp")}', interaction.guild_id)
            slowmode_change_count = await conn.fetchval(f'SELECT COUNT(*) FROM slowmode_changes WHERE guild_id = $1 {date_condition("timestamp")}', interaction.guild_id)
            bulk_delete_count = await conn.fetchval(f'SELECT COUNT(*) FROM bulk_message_deletions WHERE guild_id = $1 {date_condition("timestamp")}', interaction.guild_id)
            user_activity_count = await conn.fetchval(f'SELECT COUNT(*) FROM user_activity WHERE guild_id = $1 {date_condition("start_time")}', interaction.guild_id)
            
            # New Spotify activity stats
            spotify_song_count = await conn.fetchval(f'SELECT COUNT(DISTINCT song_title) FROM spotify_activity WHERE guild_id = $1 {date_condition("start_time")}', interaction.guild_id)
            spotify_artist_count = await conn.fetchval(f'SELECT COUNT(DISTINCT artist) FROM spotify_activity WHERE guild_id = $1 {date_condition("start_time")}', interaction.guild_id)
            spotify_album_count = await conn.fetchval(f'SELECT COUNT(DISTINCT album) FROM spotify_activity WHERE guild_id = $1 {date_condition("start_time")}', interaction.guild_id)
            spotify_listener_count = await conn.fetchval(f'SELECT COUNT(DISTINCT user_id) FROM spotify_activity WHERE guild_id = $1 {date_condition("start_time")}', interaction.guild_id)
            
        # Create the stats message
        date_range = ""
        if start_date and end_date:
            date_range = f" from {start_date} to {end_date}"
        elif start_date:
            date_range = f" from {start_date}"
        elif end_date:
            date_range = f" up to {end_date}"

        stats_message = f"""Server Stats{date_range}:
Total Messages: {message_count}
Threads Created: {thread_count}
Bans: {ban_count}
Message Edits: {edit_count}
Message Deletions: {deletion_count}
Role Changes: {role_change_count}
Nickname Changes: {nickname_change_count}
Reactions: {reaction_count}
Members Joined: {member_join_count}
Members Left: {member_leave_count}
Emoji/Sticker Changes: {emoji_sticker_change_count}
Channel Permission Changes: {channel_permission_change_count}
Slowmode Changes: {slowmode_change_count}
Bulk Message Deletions: {bulk_delete_count}
User Activity Changes: {user_activity_count}

Spotify Activity:
Unique Songs Played: {spotify_song_count}
Unique Artists Listened To: {spotify_artist_count}
Unique Albums Played: {spotify_album_count}
Number of Spotify Listeners: {spotify_listener_count}"""

        await interaction.response.send_message(stats_message, ephemeral=True)
    except ValueError:
        await interaction.response.send_message("Invalid date format. Please use YYYY-MM-DD.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in stats command: {e}\n{traceback.format_exc()}")
        await interaction.response.send_message("An error occurred while fetching stats.", ephemeral=True)

@bot.tree.command(name="spotify_top", description="Display top Spotify statistics")
@app_commands.describe(
    category="Category for top stats (songs, artists, or albums)",
    limit="Number of top items to display (default 10, max 50)",
    start_date="Start date for stats (YYYY-MM-DD, optional)",
    end_date="End date for stats (YYYY-MM-DD, optional)"
)
@app_commands.choices(category=[
    app_commands.Choice(name="Songs", value="songs"),
    app_commands.Choice(name="Artists", value="artists"),
    app_commands.Choice(name="Albums", value="albums")
])
@app_commands.guild_only()
async def spotify_top(interaction: discord.Interaction, category: str, limit: int = 10, start_date: str = None, end_date: str = None):
    try:
        limit = min(max(1, limit), 50)  # Ensure limit is between 1 and 50
        async with bot.db_pool.acquire() as conn:
            # Convert dates to timestamps if provided
            start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp()) if start_date else None
            end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp()) if end_date else None

            # Function to create the date condition for SQL queries
            def date_condition():
                if start_timestamp and end_timestamp:
                    return f"AND start_time BETWEEN {start_timestamp} AND {end_timestamp}"
                elif start_timestamp:
                    return f"AND start_time >= {start_timestamp}"
                elif end_timestamp:
                    return f"AND start_time <= {end_timestamp}"
                else:
                    return ""

            if category == "songs":
                query = f"""
                SELECT song_title, artist, COUNT(*) as play_count
                FROM spotify_activity
                WHERE guild_id = $1 {date_condition()}
                GROUP BY song_title, artist
                ORDER BY play_count DESC
                LIMIT $2
                """
            elif category == "artists":
                query = f"""
                SELECT artist, COUNT(*) as play_count
                FROM spotify_activity
                WHERE guild_id = $1 {date_condition()}
                GROUP BY artist
                ORDER BY play_count DESC
                LIMIT $2
                """
            elif category == "albums":
                query = f"""
                SELECT album, artist, COUNT(*) as play_count
                FROM spotify_activity
                WHERE guild_id = $1 {date_condition()}
                GROUP BY album, artist
                ORDER BY play_count DESC
                LIMIT $2
                """
            else:
                await interaction.response.send_message("Invalid category. Please choose songs, artists, or albums.", ephemeral=True)
                return

            results = await conn.fetch(query, interaction.guild_id, limit)

        # Create the response message
        date_range = ""
        if start_date and end_date:
            date_range = f" from {start_date} to {end_date}"
        elif start_date:
            date_range = f" from {start_date}"
        elif end_date:
            date_range = f" up to {end_date}"

        response = f"Top {limit} {category.capitalize()}{date_range}:\n\n"
        for i, row in enumerate(results, 1):
            if category == "songs":
                response += f"{i}. {row['song_title']} by {row['artist']} - {row['play_count']} plays\n"
            elif category == "artists":
                response += f"{i}. {row['artist']} - {row['play_count']} plays\n"
            elif category == "albums":
                response += f"{i}. {row['album']} by {row['artist']} - {row['play_count']} plays\n"

        await interaction.response.send_message(response, ephemeral=True)
    except ValueError:
        await interaction.response.send_message("Invalid date format. Please use YYYY-MM-DD.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in spotify_top command: {e}\n{traceback.format_exc()}")
        await interaction.response.send_message("An error occurred while fetching Spotify stats.", ephemeral=True)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandOnCooldown):
        await interaction.response.send_message(f"This command is on cooldown. Try again in {error.retry_after:.2f} seconds.", ephemeral=True)
    elif isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("You don't have the required permissions to use this command.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred while processing the command.", ephemeral=True)
        logger.error(f"Error in slash command: {error}\n{traceback.format_exc()}")


async def main():
    loop = asyncio.get_event_loop()
    
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        signal.signal(s, signal_handler)
    
    try:
        await bot.start('insert_token_here')
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Error in main: {e}\n{traceback.format_exc()}")
    finally:
        await shutdown(loop)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Exiting.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("Bot has shut down.")
        # Force close any remaining connections
        if hasattr(bot, 'http'):
            bot.http.connector._close()