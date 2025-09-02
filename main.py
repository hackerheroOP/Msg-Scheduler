import asyncio
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json
import os
from collections import defaultdict
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto, InputMediaVideo
from pyrogram.enums import ParseMode
from dotenv import load_dotenv
from aiohttp import web
import threading
from bson import ObjectId
import pytz

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot configuration
API_ID = int(os.getenv('API_ID', '1560761'))
API_HASH = os.getenv('API_HASH', 'd7e3b89b16213382fa173a9c3b5d6cc4')
BOT_TOKEN = os.getenv('BOT_TOKEN', '7984590797:AAEgnVfl6QDWTlTIpB7hWresGiTkmnbMI88')
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb+srv://wtflinksofficial:wtflinksofficial@cluster0.1uld4.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'telegram_scheduler')
ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', '1251111009'))
PORT = int(os.getenv('PORT', 8000))
# Timezone configuration
IST = pytz.timezone('Asia/Kolkata')

def get_ist_time():
    """Get current time in IST"""
    return datetime.now(IST)

def format_ist_time(dt):
    """Format datetime to IST string"""
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    return dt.astimezone(IST).strftime('%Y-%m-%d %H:%M:%S IST')

class HealthServer:
    """Dummy web server for health checks"""
    def __init__(self, port: int):
        self.port = port
        self.app = web.Application()
        self.setup_routes()
    def setup_routes(self):
        self.app.router.add_get('/', self.health_check)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/status', self.status_check)
    async def health_check(self, request):
        return web.json_response({
            'status': 'healthy',
            'service': 'telegram-scheduler-bot',
            'timestamp': get_ist_time().isoformat()
        })
    async def status_check(self, request):
        return web.json_response({
            'status': 'running',
            'service': 'telegram-scheduler-bot',
            'uptime': get_ist_time().isoformat(),
            'port': self.port
        })
    async def start_server(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        logger.info(f"Health server started on port {self.port}")

class MongoDBManager:
    def __init__(self, uri: str, db_name: str):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client[db_name]
        self.scheduled_posts = self.db.scheduled_posts
        self.bot_settings = self.db.bot_settings

    async def test_connection(self):
        try:
            await self.client.admin.command('ping')
            logger.info("MongoDB connection successful")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False

    async def add_scheduled_post(self, message_data: dict, scheduled_time: datetime, user_id: int, channel_id: str):
        """Add a post to the scheduling queue for specific channel"""
        if scheduled_time.tzinfo is None:
            scheduled_time = IST.localize(scheduled_time)
        scheduled_time_utc = scheduled_time.astimezone(pytz.utc).replace(tzinfo=None)
        post_doc = {
            'message_data': message_data,
            'scheduled_time': scheduled_time_utc,
            'status': 'pending',
            'user_id': user_id,
            'channel_id': channel_id,
            'created_at': datetime.utcnow()
        }
        result = await self.scheduled_posts.insert_one(post_doc)
        return str(result.inserted_id)

    async def get_pending_posts(self) -> List[Dict]:
        """Get all pending posts that are ready to be sent"""
        current_utc = datetime.utcnow()
        cursor = self.scheduled_posts.find({
            'status': 'pending',
            'scheduled_time': {'$lte': current_utc}
        }).sort('scheduled_time', 1)
        posts = []
        async for post in cursor:
            post['_id'] = str(post['_id'])
            posts.append(post)
        return posts

    async def mark_post_sent(self, post_id: str):
        await self.scheduled_posts.update_one(
            {'_id': ObjectId(post_id)},
            {'$set': {'status': 'sent', 'sent_at': datetime.utcnow()}}
        )

    async def check_and_cleanup_completed_channel(self, channel_id: str, user_id: int = None) -> bool:
        query = {'channel_id': channel_id, 'status': 'pending'}
        if user_id:
            query['user_id'] = user_id
        pending_count = await self.scheduled_posts.count_documents(query)
        if pending_count == 0:
            result = await self.delete_channel_data(channel_id, user_id)
            logger.info(f"Auto-cleaned channel {channel_id}: deleted {result['total_deleted']} records")
            return True
        return False

    async def get_post_stats(self, user_id: int, channel_id: str = None) -> Dict:
        query = {'user_id': user_id}
        if channel_id:
            query['channel_id'] = channel_id
        pending_count = await self.scheduled_posts.count_documents({
            **query,
            'status': 'pending'
        })
        sent_count = await self.scheduled_posts.count_documents({
            **query,
            'status': 'sent'
        })
        next_post = await self.scheduled_posts.find_one({
            **query,
            'status': 'pending'
        }, sort=[('scheduled_time', 1)])
        return {
            'pending': pending_count,
            'sent': sent_count,
            'next_post': next_post
        }

    async def clear_pending_posts(self, user_id: int, channel_id: str = None) -> int:
        query = {'user_id': user_id, 'status': 'pending'}
        if channel_id:
            query['channel_id'] = channel_id
        result = await self.scheduled_posts.delete_many(query)
        return result.deleted_count

    async def delete_channel_data(self, channel_id: str, user_id: int = None) -> Dict[str, int]:
        query = {'channel_id': channel_id}
        if user_id:
            query['user_id'] = user_id
        posts_result = await self.scheduled_posts.delete_many(query)
        settings_query = {'channel_id': channel_id}
        if user_id:
            settings_query['user_id'] = user_id
        settings_result = await self.bot_settings.delete_many(settings_query)
        return {
            'posts_deleted': posts_result.deleted_count,
            'settings_deleted': settings_result.deleted_count,
            'total_deleted': posts_result.deleted_count + settings_result.deleted_count
        }

    async def empty_database(self) -> Dict[str, int]:
        posts_result = await self.scheduled_posts.delete_many({})
        settings_result = await self.bot_settings.delete_many({})
        return {
            'posts_deleted': posts_result.deleted_count,
            'settings_deleted': settings_result.deleted_count,
            'total_deleted': posts_result.deleted_count + settings_result.deleted_count
        }

    async def get_all_channels_data(self, user_id: int = None) -> Dict:
        query = {}
        if user_id:
            query['user_id'] = user_id
        pipeline = [
            {'$match': query},
            {'$group': {
                '_id': '$channel_id',
                'pending_posts': {
                    '$sum': {'$cond': [{'$eq': ['$status', 'pending']}, 1, 0]}
                },
                'sent_posts': {
                    '$sum': {'$cond': [{'$eq': ['$status', 'sent']}, 1, 0]}
                },
                'total_posts': {'$sum': 1}
            }}
        ]
        channels_data = {}
        async for channel in self.scheduled_posts.aggregate(pipeline):
            channels_data[channel['_id']] = {
                'pending_posts': channel['pending_posts'],
                'sent_posts': channel['sent_posts'],
                'total_posts': channel['total_posts']
            }
        settings_cursor = self.bot_settings.find(query)
        async for setting in settings_cursor:
            channel_id = setting['channel_id']
            if channel_id in channels_data:
                channels_data[channel_id]['channel_name'] = setting.get('channel_name', 'Unknown')
                channels_data[channel_id]['paused'] = setting.get('paused', False)
            else:
                channels_data[channel_id] = {
                    'pending_posts': 0,
                    'sent_posts': 0,
                    'total_posts': 0,
                    'channel_name': setting.get('channel_name', 'Unknown'),
                    'paused': setting.get('paused', False)
                }
        return channels_data

    async def set_user_channel(self, user_id: int, channel_id: str, channel_name: str = None):
        """Ensure pause fields exist on upsert"""
        await self.bot_settings.update_one(
            {'user_id': user_id, 'channel_id': channel_id},
            {
                '$set': {
                    'channel_id': channel_id,
                    'channel_name': channel_name,
                    'updated_at': datetime.utcnow(),
                    'paused': False,
                    'paused_at': None
                }
            },
            upsert=True
        )

    async def get_user_channels(self, user_id: int) -> List[Dict]:
        cursor = self.bot_settings.find({'user_id': user_id})
        channels = []
        async for channel in cursor:
            channels.append(channel)
        return channels

    async def get_current_channel(self, user_id: int) -> Optional[Dict]:
        settings = await self.bot_settings.find_one({
            'user_id': user_id,
            'is_current': True
        })
        return settings

    async def set_current_channel(self, user_id: int, channel_id: str):
        await self.bot_settings.update_many(
            {'user_id': user_id},
            {'$unset': {'is_current': ''}}
        )
        await self.bot_settings.update_one(
            {'user_id': user_id, 'channel_id': channel_id},
            {'$set': {'is_current': True}},
            upsert=True
        )

    # ===== Pause/Resume helpers =====

    async def set_channel_paused(self, user_id: int, channel_id: str, paused: bool):
        """Set paused and paused_at fields for a channel."""
        update = {'paused': paused, 'updated_at': datetime.utcnow()}
        if paused:
            update['paused_at'] = datetime.utcnow()
        else:
            update['paused_at'] = None
        await self.bot_settings.update_one(
            {'user_id': user_id, 'channel_id': channel_id},
            {'$set': update},
            upsert=True
        )

    async def get_channel_setting(self, user_id: int, channel_id: str) -> Optional[Dict]:
        return await self.bot_settings.find_one({'user_id': user_id, 'channel_id': channel_id})

    async def is_channel_paused(self, user_id: int, channel_id: str) -> bool:
        s = await self.get_channel_setting(user_id, channel_id)
        return bool(s and s.get('paused'))

    async def shift_pending_posts_by(self, user_id: int, channel_id: str, delta: timedelta) -> int:
    """Shift all pending posts scheduled_time by delta (safe Python version)."""
        cursor = self.scheduled_posts.find({
        'user_id': user_id,
        'channel_id': channel_id,
        'status': 'pending'
    })
    updated = 0
        async for doc in cursor:
        old = doc.get('scheduled_time')
        
        # If scheduled_time is already corrupted ($dateAdd object), reset it
            if isinstance(old, dict):
            logger.warning(f"Corrupted scheduled_time found in post {doc['_id']}, fixing...")
            old = datetime.utcnow()  # fallback: reset to now

            new_dt = old + delta
            await self.scheduled_posts.update_one(
            {'_id': doc['_id']},
            {'$set': {'scheduled_time': new_dt}}
        )
        updated += 1
        return updated

class TelegramSchedulerBot:
    def __init__(self):
        self.app = Client(
            "scheduler_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            workdir="./session"
        )
        self.db = MongoDBManager(MONGODB_URI, DATABASE_NAME)
        self.user_last_scheduled = {}  # {user_id: {channel_id: datetime}}
        self.health_server = HealthServer(PORT)
        self.media_group_buffer = defaultdict(list)
        self.media_group_timers = {}
        os.makedirs("./session", exist_ok=True)
        self.register_handlers()

    def register_handlers(self):
        self.app.on_message(filters.command("start"))(self.start_command)
        self.app.on_message(filters.command("setchannel"))(self.set_channel_command)
        self.app.on_message(filters.command("status"))(self.status_command)
        self.app.on_message(filters.command("clear"))(self.clear_command)
        self.app.on_message(filters.command("help"))(self.help_command)
        self.app.on_message(filters.command("channels"))(self.channels_command)
        self.app.on_message(filters.command("deletechannel"))(self.delete_channel_command)
        self.app.on_message(filters.command("emptydatabase"))(self.empty_database_command)
        self.app.on_message(filters.command("allchannels"))(self.all_channels_command)
        # New pause/resume commands
        self.app.on_message(filters.command("pause"))(self.pause_command)
        self.app.on_message(filters.command("resume"))(self.resume_command)

        self.app.on_message(~filters.command(""))(self.handle_message)
        self.app.on_callback_query()(self.handle_callback)

    async def start_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        current_channel = await self.db.get_current_channel(message.from_user.id)
        channel_status = f"✅ **{current_channel.get('channel_name', 'Unknown')}**" if current_channel else "❌ **Not configured**"
        current_time = get_ist_time().strftime('%H:%M:%S IST')
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔧 Set Channel", callback_data="set_channel")],
            [InlineKeyboardButton("📊 Status", callback_data="status")],
            [InlineKeyboardButton("📺 All Channels", callback_data="channels")],
            [InlineKeyboardButton("🗑️ Management", callback_data="management")],
            [InlineKeyboardButton("❓ Help", callback_data="help")]
        ])
        welcome_text = f"""🤖 **Telegram Post Scheduler Bot**

🕐 **Current Time:** {current_time}
📺 **Active Channel:** {channel_status}

📝 **Quick Start:**
• First, set your target channel using /setchannel
• Then forward posts to me and I'll schedule them automatically
• Each post will be sent 1-3 hours after the previous one
• Switch between channels anytime with /setchannel
Use the buttons below to get started!"""
        await message.reply_text(welcome_text, reply_markup=keyboard)

    async def set_channel_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        if len(message.command) < 2:
            channels = await self.db.get_user_channels(message.from_user.id)
            if channels:
                channel_list = "📺 **Your Channels:**\n\n"
                for i, channel in enumerate(channels, 1):
                    current_mark = "🟢" if channel.get('is_current') else "⚪"
                    paused_mark = "⏸️" if channel.get('paused') else ""
                    channel_list += f"{current_mark} {paused_mark} **{i}.** {channel.get('channel_name', 'Unknown')}\n"
                    channel_list += f"    🆔 `{channel['channel_id']}`\n\n"
                channel_list += "**Usage:** `/setchannel @channel_username` or `/setchannel -1001234567890`"
                await message.reply_text(channel_list)
            else:
                await message.reply_text(
                    "📺 **Set Channel**\n\n"
                    "**Usage:** `/setchannel @channel_username` or `/setchannel -1001234567890`\n\n"
                    "**Examples:**\n"
                    "• `/setchannel @mychannel`\n"
                    "• `/setchannel -1001234567890`\n\n"
                    "💡 **Tip:** Make sure to add this bot as an admin to your channel!"
                )
            return
        channel_id = message.command[1]
        try:
            chat = await client.get_chat(channel_id)
            bot_member = await client.get_chat_member(channel_id, "me")
            if not bot_member.privileges or not bot_member.privileges.can_post_messages:
                await message.reply_text(
                    "❌ **Error:** I don't have permission to post messages in this channel.\n\n"
                    "Please add me as an admin with 'Post Messages' permission."
                )
                return
            await self.db.set_user_channel(
                message.from_user.id,
                str(chat.id),
                chat.title
            )
            await self.db.set_current_channel(message.from_user.id, str(chat.id))
            await message.reply_text(
                f"✅ **Channel configured successfully!**\n\n"
                f"📺 **Channel:** {chat.title}\n"
                f"🆔 **ID:** `{chat.id}`\n"
                f"🟢 **Status:** Current active channel\n"
                f"🕐 **Time:** {get_ist_time().strftime('%H:%M:%S IST')}\n\n"
                "Now you can start forwarding posts to me for this channel!"
            )
        except Exception as e:
            logger.error(f"Error setting channel: {e}")
            await message.reply_text(
                f"❌ **Error:** Could not access channel.\n\n"
                f"**Possible reasons:**\n"
                "• Channel doesn't exist\n"
                "• Bot is not added to the channel\n"
                "• Bot doesn't have admin rights\n\n"
                f"**Error details:** `{str(e)}`"
            )

    async def status_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        await self.show_status(message)

    async def clear_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        current_channel = await self.db.get_current_channel(message.from_user.id)
        if not current_channel:
            await message.reply_text("❌ **No active channel configured!**")
            return
        deleted_count = await self.db.clear_pending_posts(
            message.from_user.id,
            current_channel['channel_id']
        )
        if deleted_count > 0:
            if message.from_user.id not in self.user_last_scheduled:
                self.user_last_scheduled[message.from_user.id] = {}
            self.user_last_scheduled[message.from_user.id][current_channel['channel_id']] = get_ist_time()
        await message.reply_text(
            f"🗑️ **Cleared {deleted_count} pending posts!**\n\n"
            f"📺 **Channel:** {current_channel.get('channel_name', 'Unknown')}"
        )

    async def help_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        await self.show_help(message)

    async def channels_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        await self.show_channels(message)

    async def delete_channel_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        if len(message.command) < 2:
            await message.reply_text(
                "🗑️ **Delete Channel Data**\n\n"
                "**Usage:** `/deletechannel CHANNEL_ID`\n\n"
                "**Examples:**\n"
                "• `/deletechannel @mychannel`\n"
                "• `/deletechannel -1001234567890`\n\n"
                "⚠️ **Warning:** This will delete ALL posts and settings for the specified channel!"
            )
            return
        channel_id = message.command[1]
        try:
            if channel_id.startswith('@'):
                chat = await client.get_chat(channel_id)
                channel_id = str(chat.id)
        except:
            pass
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_delete")],
            [InlineKeyboardButton("🗑️ Confirm Delete", callback_data=f"confirm_delete_channel:{channel_id}")]
        ])
        await message.reply_text(
            f"⚠️ **Confirm Channel Data Deletion**\n\n"
            f"🆔 **Channel ID:** `{channel_id}`\n\n"
            f"**This will permanently delete:**\n"
            f"• All scheduled posts\n"
            f"• All sent posts history\n"
            f"• Channel settings\n\n"
            f"**Are you sure you want to proceed?**",
            reply_markup=keyboard
        )

    async def empty_database_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_empty_db")],
            [InlineKeyboardButton("💥 CONFIRM EMPTY DATABASE", callback_data="confirm_empty_db")]
        ])
        await message.reply_text(
            "🚨 **DANGEROUS OPERATION**\n\n"
            "⚠️ **YOU ARE ABOUT TO DELETE ALL DATA!**\n\n"
            "**This will permanently delete:**\n"
            "• ALL scheduled posts for ALL channels\n"
            "• ALL sent posts history for ALL channels\n"
            "• ALL channel settings\n"
            "• ALL user data\n\n"
            "🔥 **THIS ACTION CANNOT BE UNDONE!**\n\n"
            "**Are you absolutely sure you want to proceed?**",
            reply_markup=keyboard
        )

    async def all_channels_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        channels_data = await self.db.get_all_channels_data(message.from_user.id)
        if not channels_data:
            await message.reply_text("📺 **No channels found!**")
            return
        channels_text = "📊 **All Channels Overview**\n\n"
        total_pending = 0
        total_sent = 0
        for i, (channel_id, data) in enumerate(channels_data.items(), 1):
            channel_name = data.get('channel_name', 'Unknown')
            pending = data['pending_posts']
            sent = data['sent_posts']
            paused_mark = " ⏸️ Paused" if data.get('paused') else ""
            total_pending += pending
            total_sent += sent
            channels_text += f"📺 **{i}. {channel_name}**{paused_mark}\n"
            channels_text += f"🆔 `{channel_id}`\n"
            channels_text += f"⏳ Pending: {pending} | ✅ Sent: {sent}\n"
            channels_text += f"📊 Total: {data['total_posts']}\n\n"
        channels_text += f"📈 **Overall Statistics:**\n"
        channels_text += f"⏳ Total Pending: {total_pending}\n"
        channels_text += f"✅ Total Sent: {total_sent}\n"
        channels_text += f"📺 Total Channels: {len(channels_data)}"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="all_channels")],
            [InlineKeyboardButton("🗑️ Management", callback_data="management")]
        ])
        await message.reply_text(channels_text, reply_markup=keyboard)

    async def pause_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        if len(message.command) >= 2:
            channel_id = message.command[1]
            try:
                if channel_id.startswith('@'):
                    chat = await client.get_chat(channel_id)
                    channel_id = str(chat.id)
            except:
                pass
            await self.db.set_channel_paused(message.from_user.id, channel_id, True)
            await message.reply_text(f"⏸️ **Paused posting for channel** `{channel_id}`")
            return
        channels = await self.db.get_user_channels(message.from_user.id)
        if not channels:
            await message.reply_text("📺 **No channels configured**. Use `/setchannel` first.")
            return
        buttons = []
        for ch in channels:
            name = ch.get('channel_name', 'Unknown')
            cid = ch['channel_id']
            buttons.append([InlineKeyboardButton(f"⏸️ Pause {name}", callback_data=f"pause_channel:{cid}")])
        kb = InlineKeyboardMarkup(buttons)
        await message.reply_text("Choose a channel to pause:", reply_markup=kb)

    async def resume_command(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        if len(message.command) >= 2:
            channel_id = message.command[1]
            try:
                if channel_id.startswith('@'):
                    chat = await client.get_chat(channel_id)
                    channel_id = str(chat.id)
            except:
                pass
            await self._resume_channel_and_shift(message, channel_id)
            return
        channels = await self.db.get_user_channels(message.from_user.id)
        if not channels:
            await message.reply_text("📺 **No channels configured**. Use `/setchannel` first.")
            return
        buttons = []
        for ch in channels:
            name = ch.get('channel_name', 'Unknown')
            cid = ch['channel_id']
            buttons.append([InlineKeyboardButton(f"▶️ Resume {name}", callback_data=f"resume_channel:{cid}")])
        kb = InlineKeyboardMarkup(buttons)
        await message.reply_text("Choose a channel to resume:", reply_markup=kb)

    async def _resume_channel_and_shift(self, msg_or_like: Message, channel_id: str):
        setting = await self.db.get_channel_setting(msg_or_like.from_user.id, channel_id)
        if not setting or not setting.get('paused'):
            await msg_or_like.reply_text("ℹ️ Channel is not paused.")
            return
        paused_at = setting.get('paused_at')
        if not paused_at:
            await self.db.set_channel_paused(msg_or_like.from_user.id, channel_id, False)
            await msg_or_like.reply_text(f"▶️ **Resumed** channel `{channel_id}` (no shift applied)")
            return
        now = datetime.utcnow()
        pause_duration = now - paused_at
        updated = await self.db.shift_pending_posts_by(msg_or_like.from_user.id, channel_id, pause_duration)
        await self.db.set_channel_paused(msg_or_like.from_user.id, channel_id, False)
        # Reset base so future schedules start from now
        if msg_or_like.from_user.id in self.user_last_scheduled:
            self.user_last_scheduled[msg_or_like.from_user.id].pop(channel_id, None)
        await msg_or_like.reply_text(
            f"▶️ **Resumed** channel `{channel_id}`\n"
            f"⏱️ Shifted {updated} pending posts by {int(pause_duration.total_seconds())} seconds"
        )

    async def handle_callback(self, client: Client, callback_query: CallbackQuery):
        if callback_query.from_user.id != ADMIN_USER_ID:
            await callback_query.answer("❌ Unauthorized access!", show_alert=True)
            return
        data = callback_query.data
        if data == "set_channel":
            await callback_query.message.edit_text(
                "📺 **Set Channel**\n\n"
                "**Usage:** `/setchannel @channel_username` or `/setchannel -1001234567890`\n\n"
                "**Examples:**\n"
                "• `/setchannel @mychannel`\n"
                "• `/setchannel -1001234567890`\n\n"
                "💡 **Tip:** Make sure to add this bot as an admin to your channel!"
            )
        elif data == "status":
            await self.show_status(callback_query.message, edit=True)
        elif data == "channels":
            await self.show_channels(callback_query.message, edit=True)
        elif data == "help":
            await self.show_help(callback_query.message, edit=True)
        elif data == "management":
            await self.show_management_menu(callback_query.message, edit=True)
        elif data == "all_channels":
            channels_data = await self.db.get_all_channels_data(callback_query.from_user.id)
            if not channels_data:
                await callback_query.message.edit_text("📺 **No channels found!**")
                await callback_query.answer()
                return
            channels_text = "📊 **All Channels Overview**\n\n"
            total_pending = 0
            total_sent = 0
            for i, (channel_id, data) in enumerate(channels_data.items(), 1):
                channel_name = data.get('channel_name', 'Unknown')
                pending = data['pending_posts']
                sent = data['sent_posts']
                paused_mark = " ⏸️ Paused" if data.get('paused') else ""
                total_pending += pending
                total_sent += sent
                channels_text += f"📺 **{i}. {channel_name}**{paused_mark}\n"
                channels_text += f"🆔 `{channel_id}`\n"
                channels_text += f"⏳ Pending: {pending} | ✅ Sent: {sent}\n"
                channels_text += f"📊 Total: {data['total_posts']}\n\n"
            channels_text += f"📈 **Overall Statistics:**\n"
            channels_text += f"⏳ Total Pending: {total_pending}\n"
            channels_text += f"✅ Total Sent: {total_sent}\n"
            channels_text += f"📺 Total Channels: {len(channels_data)}"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="all_channels")],
                [InlineKeyboardButton("🗑️ Management", callback_data="management")]
            ])
            await callback_query.message.edit_text(channels_text, reply_markup=keyboard)
        elif data.startswith("confirm_delete_channel:"):
            channel_id = data.split(":", 1)[1]
            result = await self.db.delete_channel_data(channel_id, callback_query.from_user.id)
            if callback_query.from_user.id in self.user_last_scheduled:
                self.user_last_scheduled[callback_query.from_user.id].pop(channel_id, None)
            await callback_query.message.edit_text(
                f"✅ **Channel data deleted successfully!**\n\n"
                f"🆔 **Channel ID:** `{channel_id}`\n"
                f"🗑️ **Posts deleted:** {result['posts_deleted']}\n"
                f"⚙️ **Settings deleted:** {result['settings_deleted']}\n"
                f"📊 **Total deleted:** {result['total_deleted']}\n"
                f"🕐 **Time:** {get_ist_time().strftime('%Y-%m-%d %H:%M:%S IST')}"
            )
        elif data == "confirm_empty_db":
            result = await self.db.empty_database()
            self.user_last_scheduled.clear()
            await callback_query.message.edit_text(
                f"💥 **DATABASE EMPTIED SUCCESSFULLY!**\n\n"
                f"🗑️ **Posts deleted:** {result['posts_deleted']}\n"
                f"⚙️ **Settings deleted:** {result['settings_deleted']}\n"
                f"📊 **Total deleted:** {result['total_deleted']}\n"
                f"🕐 **Time:** {get_ist_time().strftime('%Y-%m-%d %H:%M:%S IST')}\n\n"
                f"🔄 **All data has been permanently deleted!**"
            )
        elif data in ["cancel_delete", "cancel_empty_db"]:
            await callback_query.message.edit_text("❌ **Operation cancelled.**")
        elif data.startswith("pause_channel:"):
            channel_id = data.split(":", 1)[1]
            await self.db.set_channel_paused(callback_query.from_user.id, channel_id, True)
            await callback_query.message.edit_text(f"⏸️ **Paused** channel `{channel_id}`")
        elif data.startswith("resume_channel:"):
            channel_id = data.split(":", 1)[1]
            class _M:
                from_user = type("U", (), {"id": callback_query.from_user.id})
                async def reply_text(self_inner, txt, **kwargs):
                    await callback_query.message.edit_text(txt)
            m = _M()
            await self._resume_channel_and_shift(m, channel_id)
        elif data == "pause_current":
            current_channel = await self.db.get_current_channel(callback_query.from_user.id)
            if not current_channel:
                await callback_query.answer("No active channel", show_alert=True)
            else:
                await self.db.set_channel_paused(callback_query.from_user.id, current_channel['channel_id'], True)
                await callback_query.message.edit_text("⏸️ Paused current channel.")
        elif data == "resume_current":
            current_channel = await self.db.get_current_channel(callback_query.from_user.id)
            if not current_channel:
                await callback_query.answer("No active channel", show_alert=True)
            else:
                class _M:
                    from_user = type("U", (), {"id": callback_query.from_user.id})
                    async def reply_text(self_inner, txt, **kwargs):
                        await callback_query.message.edit_text(txt)
                m = _M()
                await self._resume_channel_and_shift(m, current_channel['channel_id'])
        await callback_query.answer()

    async def show_management_menu(self, message: Message, edit: bool = False):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 All Channels Overview", callback_data="all_channels")],
            [InlineKeyboardButton("🔄 Back to Main", callback_data="start")]
        ])
        management_text = """🗑️ **Data Management**

**Available Commands:**

🗑️ `/deletechannel CHANNEL_ID` - Delete all data for a specific channel
💥 `/emptydatabase` - Delete ALL data from database
📊 `/allchannels` - View all channels overview

**Quick Actions:**"""
        if edit:
            await message.edit_text(management_text, reply_markup=keyboard)
        else:
            await message.reply_text(management_text, reply_markup=keyboard)

    async def show_status(self, message: Message, edit: bool = False):
        current_channel = await self.db.get_current_channel(message.from_user.id)
        current_time = get_ist_time().strftime('%H:%M:%S IST')
        if not current_channel:
            status_text = f"❌ **No active channel**\n\n🕐 **Current Time:** {current_time}\n\nUse `/setchannel` to set up your channel first."
        else:
            stats = await self.db.get_post_stats(message.from_user.id, current_channel['channel_id'])
            paused = await self.db.is_channel_paused(message.from_user.id, current_channel['channel_id'])
            paused_mark = "⏸️ Paused" if paused else "🟢 Active"
            status_text = f"📊 **Scheduling Status**\n\n"
            status_text += f"🕐 **Current Time:** {current_time}\n"
            status_text += f"{paused_mark} **Channel:** {current_channel.get('channel_name', 'Unknown')}\n"
            status_text += f"🆔 **Channel ID:** `{current_channel['channel_id']}`\n\n"
            status_text += f"⏳ **Pending posts:** {stats['pending']}\n"
            status_text += f"✅ **Sent posts:** {stats['sent']}\n"
            if stats['next_post']:
                next_time_utc = stats['next_post']['scheduled_time']
                next_time_ist = pytz.utc.localize(next_time_utc).astimezone(IST)
                status_text += f"⏰ **Next post:** {next_time_ist.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
            else:
                status_text += "⏰ **Next post:** No posts scheduled\n"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="status")],
            [InlineKeyboardButton("⏸️ Pause", callback_data="pause_current"),
             InlineKeyboardButton("▶️ Resume", callback_data="resume_current")],
            [InlineKeyboardButton("🔧 Switch Channel", callback_data="set_channel")],
            [InlineKeyboardButton("📺 All Channels", callback_data="channels")]
        ])
        if edit:
            await message.edit_text(status_text, reply_markup=keyboard)
        else:
            await message.reply_text(status_text, reply_markup=keyboard)

    async def show_channels(self, message: Message, edit: bool = False):
        channels = await self.db.get_user_channels(message.from_user.id)
        if not channels:
            channels_text = "📺 **No channels configured**\n\nUse `/setchannel` to add your first channel."
        else:
            channels_text = "📺 **All Your Channels:**\n\n"
            for i, channel in enumerate(channels, 1):
                current_mark = "🟢" if channel.get('is_current') else "⚪"
                paused_mark = "⏸️" if channel.get('paused') else ""
                stats = await self.db.get_post_stats(message.from_user.id, channel['channel_id'])
                channels_text += f"{current_mark} {paused_mark} **{i}.** {channel.get('channel_name', 'Unknown')}\n"
                channels_text += f"    🆔 `{channel['channel_id']}`\n"
                channels_text += f"    ⏳ Pending: {stats['pending']} | ✅ Sent: {stats['sent']}\n\n"
            channels_text += "💡 **Tip:** Use `/setchannel CHANNEL_ID` to switch active channel"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="channels")],
            [InlineKeyboardButton("🔧 Add Channel", callback_data="set_channel")]
        ])
        if edit:
            await message.edit_text(channels_text, reply_markup=keyboard)
        else:
            await message.reply_text(channels_text, reply_markup=keyboard)

    async def show_help(self, message: Message, edit: bool = False):
        help_text = """❓ **Help & Commands**

**🔧 Setup Commands:**
• `/start` - Start the bot and see main menu
• `/setchannel @channel` - Set/switch target channel for posting
• `/status` - Check scheduling status for active channel
• `/channels` - View all configured channels
• `/clear` - Clear pending posts for active channel
• `/pause [CHANNEL_ID or @username]` - Pause posting for a channel
• `/resume [CHANNEL_ID or @username]` - Resume and shift pending posts by pause duration
• `/help` - Show this help message

**🗑️ Management Commands:**
• `/deletechannel CHANNEL_ID` - Delete all data for specific channel
• `/emptydatabase` - Delete ALL data from database (DANGEROUS!)
• `/allchannels` - View overview of all channels

**📝 How to Use:**
1. Set your channel with `/setchannel @yourchannel`
2. Forward any post to me (photos, videos, albums, text)
3. I'll automatically schedule them with 1-3 hour intervals
4. Posts will be sent to your active channel automatically
5. Switch channels anytime with `/setchannel`

**📋 Supported Content:**
• 📸 Photos with captions
• 🎥 Videos with captions
• 📁 Albums (media groups) with captions
• 📄 Documents
• 💬 Text messages
• 🔗 Messages with links

**⚡ Multi-Channel Features:**
• Manage multiple channels independently
• Each channel has its own post queue
• Separate scheduling timers for each channel
• Switch between channels instantly
• All times displayed in IST (Indian Standard Time)"""
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Main Menu", callback_data="start")]
        ])
        if edit:
            await message.edit_text(help_text, reply_markup=keyboard)
        else:
            await message.reply_text(help_text, reply_markup=keyboard)

    def calculate_next_schedule_time(self, user_id: int, channel_id: str) -> datetime:
        if user_id not in self.user_last_scheduled:
            self.user_last_scheduled[user_id] = {}
        if channel_id not in self.user_last_scheduled[user_id]:
            base_time = get_ist_time()
            logger.info(f"Starting fresh schedule for channel {channel_id} from current time: {base_time}")
        else:
            base_time = self.user_last_scheduled[user_id][channel_id]
        random_hours = random.uniform(1, 2)
        random_minutes = random.randint(0, 26)
        next_time = base_time + timedelta(hours=random_hours, minutes=random_minutes)
        self.user_last_scheduled[user_id][channel_id] = next_time
        return next_time

    async def handle_message(self, client: Client, message: Message):
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        current_channel = await self.db.get_current_channel(message.from_user.id)
        if not current_channel:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔧 Set Channel", callback_data="set_channel")]
            ])
            await message.reply_text(
                "❌ **No active channel configured!**\n\n"
                "Please set your target channel first using `/setchannel @yourchannel`",
                reply_markup=keyboard
            )
            return
        # Optional: warn if channel is paused
        if await self.db.is_channel_paused(message.from_user.id, current_channel['channel_id']):
            await message.reply_text("⏸️ **Channel is paused.** New posts will be queued but sending is paused.")
        if message.media_group_id:
            await self.handle_media_group(message, current_channel)
        else:
            await self.schedule_single_message(message, current_channel)

    async def handle_media_group(self, message: Message, channel_settings: dict):
        group_id = message.media_group_id
        message_data = {
            'type': None,
            'content': None,
            'caption': message.caption or '',
            'channel_id': channel_settings['channel_id']
        }
        if message.photo:
            message_data['type'] = 'photo'
            message_data['content'] = message.photo.file_id
        elif message.video:
            message_data['type'] = 'video'
            message_data['content'] = message.video.file_id
        elif message.animation:
            message_data['type'] = 'animation'
            message_data['content'] = message.animation.file_id
        elif message.document:
            message_data['type'] = 'document'
            message_data['content'] = message.document.file_id
        self.media_group_buffer[group_id].append(message_data)
        if group_id in self.media_group_timers:
            self.media_group_timers[group_id].cancel()
        self.media_group_timers[group_id] = asyncio.create_task(
            self.process_media_group_after_delay(group_id, channel_settings, message)
        )

    async def process_media_group_after_delay(self, group_id: str, channel_settings: dict, sample_message: Message):
        await asyncio.sleep(2)
        if group_id in self.media_group_buffer:
            media_items = self.media_group_buffer[group_id]
            album_caption = ""
            for item in media_items:
                if item['caption']:
                    album_caption = item['caption']
                    break
            media_group_data = {
                'type': 'media_group',
                'items': media_items,
                'caption': album_caption,
                'channel_id': channel_settings['channel_id']
            }
            scheduled_time = self.calculate_next_schedule_time(
                sample_message.from_user.id,
                channel_settings['channel_id']
            )
            post_id = await self.db.add_scheduled_post(
                media_group_data,
                scheduled_time,
                sample_message.from_user.id,
                channel_settings['channel_id']
            )
            del self.media_group_buffer[group_id]
            if group_id in self.media_group_timers:
                del self.media_group_timers[group_id]
            caption_info = f"📝 **Caption:** {album_caption[:50]}..." if album_caption else "📝 **Caption:** None"
            await sample_message.reply_text(
                f"📅 **Album scheduled successfully!**\n\n"
                f"📸 **Media items:** {len(media_items)}\n"
                f"{caption_info}\n"
                f"📺 **Channel:** {channel_settings.get('channel_name', 'Unknown')}\n"
                f"⏰ **Scheduled for:** {scheduled_time.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
                f"🆔 **Post ID:** `{post_id}`"
            )

    async def schedule_single_message(self, message: Message, channel_settings: dict):
        message_data = {
            'type': None,
            'content': None,
            'caption': message.caption or '',
            'channel_id': channel_settings['channel_id']
        }
        if message.photo:
            message_data['type'] = 'photo'
            message_data['content'] = message.photo.file_id
        elif message.video:
            message_data['type'] = 'video'
            message_data['content'] = message.video.file_id
        elif message.animation:
            message_data['type'] = 'animation'
            message_data['content'] = message.animation.file_id
        elif message.document:
            message_data['type'] = 'document'
            message_data['content'] = message.document.file_id
        elif message.text:
            message_data['type'] = 'text'
            message_data['content'] = message.text
        else:
            await message.reply_text("❌ **Unsupported message type!**")
            return
        scheduled_time = self.calculate_next_schedule_time(
            message.from_user.id,
            channel_settings['channel_id']
        )
        post_id = await self.db.add_scheduled_post(
            message_data,
            scheduled_time,
            message.from_user.id,
            channel_settings['channel_id']
        )
        caption_info = f"📝 **Caption:** {message_data['caption'][:50]}..." if message_data['caption'] else "📝 **Caption:** None"
        await message.reply_text(
            f"📅 **Post scheduled successfully!**\n\n"
            f"{caption_info}\n"
            f"📺 **Channel:** {channel_settings.get('channel_name', 'Unknown')}\n"
            f"⏰ **Scheduled for:** {scheduled_time.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
            f"🆔 **Post ID:** `{post_id}`"
        )

    async def send_scheduled_posts(self):
        try:
            pending_posts = await self.db.get_pending_posts()
            for post in pending_posts:
                try:
                    message_data = post['message_data']
                    channel_id = message_data['channel_id']
                    user_id = post['user_id']
                    # Skip paused channels
                    if await self.db.is_channel_paused(user_id, channel_id):
                        continue
                    # Send the post
                    if message_data['type'] == 'media_group':
                        media_list = []
                        album_caption = message_data.get('caption', '')
                        for i, item in enumerate(message_data['items']):
                            if item['type'] == 'photo':
                                media_list.append(InputMediaPhoto(
                                    media=item['content'],
                                    caption=album_caption if i == 0 else ''
                                ))
                            elif item['type'] == 'video':
                                media_list.append(InputMediaVideo(
                                    media=item['content'],
                                    caption=album_caption if i == 0 else ''
                                ))
                            elif item['type'] == 'animation':
                                media_list.append(InputMediaPhoto(
                                    media=item['content'],
                                    caption=album_caption if i == 0 else ''
                                ))
                            elif item['type'] == 'document':
                                media_list.append(InputMediaPhoto(
                                    media=item['content'],
                                    caption=album_caption if i == 0 else ''
                                ))
                        if media_list:
                            await self.app.send_media_group(
                                chat_id=channel_id,
                                media=media_list
                            )
                    elif message_data['type'] == 'photo':
                        await self.app.send_photo(
                            chat_id=channel_id,
                            photo=message_data['content'],
                            caption=message_data['caption']
                        )
                    elif message_data['type'] == 'video':
                        await self.app.send_video(
                            chat_id=channel_id,
                            video=message_data['content'],
                            caption=message_data['caption']
                        )
                    elif message_data['type'] == 'animation':
                        await self.app.send_animation(
                            chat_id=channel_id,
                            animation=message_data['content'],
                            caption=message_data['caption']
                        )
                    elif message_data['type'] == 'document':
                        await self.app.send_document(
                            chat_id=channel_id,
                            document=message_data['content'],
                            caption=message_data['caption']
                        )
                    elif message_data['type'] == 'text':
                        await self.app.send_message(
                            chat_id=channel_id,
                            text=message_data['content']
                        )
                    await self.db.mark_post_sent(post['_id'])
                    logger.info(f"Sent scheduled post {post['_id']} to channel {channel_id} at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S IST')}")
                    cleanup_happened = await self.db.check_and_cleanup_completed_channel(channel_id, user_id)
                    if cleanup_happened and user_id in self.user_last_scheduled and channel_id in self.user_last_scheduled[user_id]:
                        del self.user_last_scheduled[user_id][channel_id]
                        logger.info(f"Cleared scheduling cache for channel {channel_id} - next posts will start from current time")
                except Exception as e:
                    logger.error(f"Error sending post {post['_id']}: {e}")
        except Exception as e:
            logger.error(f"Error in send_scheduled_posts: {e}")

    async def start_scheduler(self):
        while True:
            try:
                await self.send_scheduled_posts()
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(60)

    async def run(self):
        if not all([API_ID, API_HASH, BOT_TOKEN, MONGODB_URI, ADMIN_USER_ID]):
            logger.error("Missing required environment variables!")
            return
        if not await self.db.test_connection():
            logger.error("Failed to connect to MongoDB. Exiting...")
            return
        await self.health_server.start_server()
        logger.info(f"Health server started on port {PORT}")
        await self.app.start()
        logger.info(f"Telegram bot started successfully at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S IST')}")
        asyncio.create_task(self.start_scheduler())
        await asyncio.Event().wait()

    # UI helpers kept at end for clarity (no change except added pause/resume buttons in status)
    # show_management_menu, show_status, show_channels, show_help already above.

async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN, MONGODB_URI, ADMIN_USER_ID]):
        logger.error("Missing required environment variables!")
        return
    bot = TelegramSchedulerBot()
    await bot.run()

if __name__ == '__main__':
    asyncio.run(main())
