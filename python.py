import asyncio
import logging
import os
from datetime import datetime, timedelta
import pytz
from typing import Optional, Dict, Any, List
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, UserIsBlocked, PeerIdInvalid
import json
from aiohttp import web
from contextlib import asynccontextmanager

# Configuration
API_ID = int(os.getenv('API_ID', '1560761'))
API_HASH = os.getenv('API_HASH', 'd7e3b89b16213382fa173a9c3b5d6cc4')
BOT_TOKEN = os.getenv('BOT_TOKEN', '7984590797:AAEgnVfl6QDWTlTIpB7hWresGiTkmnbMI88')
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb+srv://wtflinksofficial:wtflinksofficial@cluster0.1uld4.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'telegram_scheduler')
ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', '1251111009'))
PORT = int(os.getenv('PORT', 8000))
# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

def get_ist_time():
    """Get current time in IST"""
    return datetime.now(IST)

def format_ist_time(dt):
    """Format datetime to IST string"""
    return dt.astimezone(IST).strftime('%Y-%m-%d %H:%M:%S IST')

class HealthServer:
    def __init__(self, port: int = 8080):
        self.port = port
        self.app = web.Application()
        self.setup_routes()
        
    def setup_routes(self):
        self.app.router.add_get('/', self.health_check)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/status', self.status_check)
    
    async def health_check(self, request):
        return web.json_response({'status': 'healthy', 'timestamp': datetime.now().isoformat()})
    
    async def status_check(self, request):
        return web.json_response({
            'service': 'Telegram Scheduler Bot',
            'status': 'running',
            'timestamp': datetime.now().isoformat()
        })
    
    async def start_server(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        logger.info(f"Health server started on port {self.port}")

class MongoDBManager:
    def __init__(self, uri: str, db_name: str = "telegram_scheduler"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.scheduled_posts = self.db.scheduled_posts
        self.bot_settings = self.db.bot_settings
        
    async def test_connection(self) -> bool:
        """Test MongoDB connection"""
        try:
            self.client.admin.command('ping')
            logger.info("MongoDB connection successful")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False
    
    async def add_scheduled_post(self, user_id: int, channel_id: str, 
                               message_type: str, content: dict, 
                               scheduled_time: datetime) -> str:
        """Add a scheduled post to the database"""
        post_data = {
            'user_id': user_id,
            'channel_id': channel_id,
            'message_type': message_type,
            'content': content,
            'scheduled_time': scheduled_time,
            'created_at': datetime.utcnow(),
            'status': 'pending'
        }
        
        result = self.scheduled_posts.insert_one(post_data)
        logger.info(f"Added scheduled post: {result.inserted_id}")
        return str(result.inserted_id)
    
    async def get_pending_posts(self) -> List[dict]:
        """Get all pending posts that are ready to be sent"""
        current_time = datetime.utcnow()
        return list(self.scheduled_posts.find({
            'status': 'pending',
            'scheduled_time': {'$lte': current_time}
        }))
    
    async def mark_post_sent(self, post_id: str):
        """Mark a post as sent"""
        self.scheduled_posts.update_one(
            {'_id': pymongo.ObjectId(post_id)},
            {
                '$set': {
                    'status': 'sent',
                    'sent_at': datetime.utcnow()
                }
            }
        )
    
    async def get_post_stats(self, user_id: int, channel_id: str) -> Dict:
        """Get post statistics for a user and channel"""
        pending_count = self.scheduled_posts.count_documents({
            'user_id': user_id,
            'channel_id': channel_id,
            'status': 'pending'
        })
        
        sent_count = self.scheduled_posts.count_documents({
            'user_id': user_id,
            'channel_id': channel_id,
            'status': 'sent'
        })
        
        next_post = self.scheduled_posts.find_one({
            'user_id': user_id,
            'channel_id': channel_id,
            'status': 'pending'
        }, sort=[('scheduled_time', 1)])
        
        return {
            'pending': pending_count,
            'sent': sent_count,
            'next_scheduled': next_post['scheduled_time'] if next_post else None
        }
    
    async def clear_pending_posts(self, user_id: int, channel_id: str) -> int:
        """Clear all pending posts for a user and channel"""
        result = self.scheduled_posts.delete_many({
            'user_id': user_id,
            'channel_id': channel_id,
            'status': 'pending'
        })
        return result.deleted_count
    
    async def set_user_channel(self, user_id: int, channel_id: str, channel_name: str):
        """Set or update user's channel"""
        self.bot_settings.update_one(
            {'user_id': user_id, 'channel_id': channel_id},
            {
                '$set': {
                    'channel_name': channel_name,
                    'updated_at': datetime.utcnow(),
                    'is_current': True
                }
            },
            upsert=True
        )
        
        # Set all other channels for this user as not current
        self.bot_settings.update_many(
            {
                'user_id': user_id, 
                'channel_id': {'$ne': channel_id}
            },
            {'$set': {'is_current': False}}
        )
    
    async def get_user_channels(self, user_id: int) -> List[dict]:
        """Get all channels for a user"""
        return list(self.bot_settings.find({
            'user_id': user_id
        }, sort=[('updated_at', -1)]))
    
    async def get_current_channel(self, user_id: int) -> Optional[dict]:
        """Get current channel for a user"""
        return self.bot_settings.find_one({
            'user_id': user_id,
            'is_current': True
        })
    
    async def set_current_channel(self, user_id: int, channel_id: str):
        """Set current channel for a user"""
        # First, set all channels as not current
        self.bot_settings.update_many(
            {'user_id': user_id},
            {'$set': {'is_current': False}}
        )
        
        # Then set the specified channel as current
        self.bot_settings.update_one(
            {'user_id': user_id, 'channel_id': channel_id},
            {'$set': {'is_current': True}}
        )

    async def clear_channel_data(self, user_id: int, channel_id: str) -> Dict[str, int]:
        """Clear all data related to a channel including scheduled posts and channel settings"""
        try:
            # Delete all scheduled posts for the channel (both pending and sent)
            posts_result = self.scheduled_posts.delete_many({
                'user_id': user_id, 
                'channel_id': channel_id
            })
            
            # Delete the channel settings entry
            settings_result = self.bot_settings.delete_one({
                'user_id': user_id, 
                'channel_id': channel_id
            })
            
            # Return detailed count of deleted items
            return {
                'posts_deleted': posts_result.deleted_count,
                'channel_deleted': settings_result.deleted_count,
                'total_deleted': posts_result.deleted_count + settings_result.deleted_count
            }
            
        except Exception as e:
            logger.error(f"Error clearing channel data: {e}")
            return {'posts_deleted': 0, 'channel_deleted': 0, 'total_deleted': 0}

class TelegramSchedulerBot:
    def __init__(self):
        self.app = Client("scheduler_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
        self.db = MongoDBManager(MONGODB_URI)
        self.media_group_buffer = {}
        self.user_last_scheduled = {}
        self.register_handlers()
    
    def register_handlers(self):
        """Register all message and callback handlers"""
        self.app.on_message(filters.command("start"))(self.start_command)
        self.app.on_message(filters.command("setchannel"))(self.set_channel_command)
        self.app.on_message(filters.command("status"))(self.status_command)
        self.app.on_message(filters.command("clear"))(self.clear_command)
        self.app.on_message(filters.command("deletechannel"))(self.delete_channel_command)
        self.app.on_message(filters.command("help"))(self.help_command)
        self.app.on_message(filters.command("channels"))(self.channels_command)
        self.app.on_message(~filters.command(""))(self.handle_message)
        self.app.on_callback_query()(self.handle_callback)
    
    async def start_command(self, client: Client, message: Message):
        """Handle /start command"""
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📊 Status", callback_data="status"),
                InlineKeyboardButton("📺 Channels", callback_data="channels")
            ],
            [
                InlineKeyboardButton("❓ Help", callback_data="help"),
                InlineKeyboardButton("🗑️ Clear Queue", callback_data="clear")
            ]
        ])
        
        await message.reply_text(
            f"🤖 **Telegram Scheduler Bot**\n\n"
            f"🕐 **Current Time:** {get_ist_time().strftime('%H:%M:%S IST')}\n"
            f"👤 **Admin:** {message.from_user.first_name}\n\n"
            f"**What can I do?**\n"
            f"• Schedule posts to channels automatically\n"
            f"• Handle multiple channels with separate queues\n"
            f"• Random scheduling (1-3 hours between posts)\n"
            f"• Support all media types and albums\n\n"
            f"**Quick Start:**\n"
            f"1. Set channel: `/setchannel @yourchannel`\n"
            f"2. Forward any post to me\n"
            f"3. I'll schedule it automatically!\n\n"
            f"Use the buttons below to navigate:",
            reply_markup=keyboard
        )
    
    async def set_channel_command(self, client: Client, message: Message):
        """Handle /setchannel command"""
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        
        if len(message.command) < 2:
            await message.reply_text(
                "📺 **Set Channel**\n\n"
                "**Usage:** `/setchannel @channel_username`\n"
                "**Example:** `/setchannel @mychannel`\n\n"
                "⚠️ **Important:**\n"
                "• Make sure the bot is admin in the channel\n"
                "• Channel must be public or bot must be added\n"
                "• Use @ before channel username"
            )
            return
        
        channel_username = message.command[1]
        if not channel_username.startswith('@'):
            channel_username = '@' + channel_username
        
        try:
            # Get channel info
            channel = await client.get_chat(channel_username)
            channel_id = str(channel.id)
            
            # Check if bot is admin
            member = await client.get_chat_member(channel.id, "me")
            if member.status not in ["creator", "administrator"]:
                await message.reply_text(
                    f"❌ **Permission Error**\n\n"
                    f"I'm not an admin in **{channel.title}**\n"
                    f"Please make me an admin with post permissions."
                )
                return
            
            # Save channel settings
            await self.db.set_user_channel(message.from_user.id, channel_id, channel.title)
            
            await message.reply_text(
                f"✅ **Channel set successfully!**\n\n"
                f"📺 **Channel:** {channel.title}\n"
                f"🆔 **Channel ID:** `{channel_id}`\n"
                f"👥 **Members:** {getattr(channel, 'members_count', 'Private')}\n"
                f"🕐 **Set at:** {get_ist_time().strftime('%H:%M:%S IST')}\n\n"
                f"Now forward any message to me and I'll schedule it for this channel!"
            )
            
        except Exception as e:
            await message.reply_text(
                f"❌ **Error setting channel**\n\n"
                f"**Error:** {str(e)}\n\n"
                f"**Common issues:**\n"
                f"• Channel doesn't exist\n"
                f"• Bot not added to channel\n"
                f"• Channel is private\n"
                f"• Wrong username format"
            )
    
    async def status_command(self, client: Client, message: Message):
        """Handle /status command"""
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        
        await self.show_status(message)
    
    async def clear_command(self, client: Client, message: Message):
        """Handle /clear command"""
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        
        current_channel = await self.db.get_current_channel(message.from_user.id)
        if not current_channel:
            await message.reply_text("❌ **No active channel set!**\n\nUse `/setchannel @channel` first.")
            return
        
        cleared_count = await self.db.clear_pending_posts(
            message.from_user.id, 
            current_channel['channel_id']
        )
        
        await message.reply_text(
            f"🗑️ **Queue cleared!**\n\n"
            f"📺 **Channel:** {current_channel['channel_name']}\n"
            f"🗂️ **Posts removed:** {cleared_count}\n"
            f"🕐 **Time:** {get_ist_time().strftime('%H:%M:%S IST')}"
        )

    async def delete_channel_command(self, client: Client, message: Message):
        """Handle /deletechannel command"""
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        
        if len(message.command) < 2:
            await message.reply_text(
                "🗑️ **Delete Channel Data**\n\n"
                "**Usage:** `/deletechannel CHANNEL_ID`\n\n"
                "**Example:** `/deletechannel -1001234567890`\n\n"
                "⚠️ **Warning:** This will permanently delete:\n"
                "• All scheduled posts for the channel\n"
                "• All sent post history\n"
                "• Channel configuration\n\n"
                "Use `/channels` to see your configured channels."
            )
            return
        
        channel_id = message.command[1]
        
        # Verify the channel belongs to the user
        channels = await self.db.get_user_channels(message.from_user.id)
        channel_exists = any(ch['channel_id'] == channel_id for ch in channels)
        
        if not channel_exists:
            await message.reply_text(
                "❌ **Channel not found!**\n\n"
                f"Channel ID `{channel_id}` is not configured for your account.\n"
                "Use `/channels` to see your configured channels."
            )
            return
        
        # Get channel name for confirmation
        channel_name = next(
            (ch.get('channel_name', 'Unknown') for ch in channels if ch['channel_id'] == channel_id),
            'Unknown'
        )
        
        # Clear all channel data
        result = await self.db.clear_channel_data(message.from_user.id, channel_id)
        
        if result['total_deleted'] > 0:
            # If this was the current channel, clear the current selection
            current_channel = await self.db.get_current_channel(message.from_user.id)
            if current_channel and current_channel['channel_id'] == channel_id:
                # No need to explicitly clear current flag as the entire record is deleted
                pass
            
            # Clear the scheduling timer for this channel
            if message.from_user.id in self.user_last_scheduled:
                if channel_id in self.user_last_scheduled[message.from_user.id]:
                    del self.user_last_scheduled[message.from_user.id][channel_id]
            
            await message.reply_text(
                f"🗑️ **Channel data cleared successfully!**\n\n"
                f"📺 **Channel:** {channel_name}\n"
                f"🆔 **Channel ID:** `{channel_id}`\n"
                f"📝 **Posts deleted:** {result['posts_deleted']}\n"
                f"⚙️ **Settings deleted:** {result['channel_deleted']}\n"
                f"🕐 **Time:** {get_ist_time().strftime('%H:%M:%S IST')}\n\n"
                "✅ All data for this channel has been permanently removed."
            )
        else:
            await message.reply_text(
                f"⚠️ **No data found to delete!**\n\n"
                f"📺 **Channel:** {channel_name}\n"
                f"🆔 **Channel ID:** `{channel_id}`"
            )
    
    async def help_command(self, client: Client, message: Message):
        """Handle /help command"""
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        
        await self.show_help(message)
    
    async def channels_command(self, client: Client, message: Message):
        """Handle /channels command"""
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        
        await self.show_channels(message)
    
    async def handle_callback(self, client: Client, callback_query: CallbackQuery):
        """Handle callback queries from inline keyboards"""
        if callback_query.from_user.id != ADMIN_USER_ID:
            await callback_query.answer("❌ Unauthorized!", show_alert=True)
            return
        
        data = callback_query.data
        
        if data == "status":
            await self.show_status(callback_query.message)
        elif data == "channels":
            await self.show_channels(callback_query.message)
        elif data == "help":
            await self.show_help(callback_query.message)
        elif data == "clear":
            current_channel = await self.db.get_current_channel(callback_query.from_user.id)
            if current_channel:
                cleared_count = await self.db.clear_pending_posts(
                    callback_query.from_user.id, 
                    current_channel['channel_id']
                )
                await callback_query.message.edit_text(
                    f"🗑️ **Queue cleared!**\n\n"
                    f"📺 **Channel:** {current_channel['channel_name']}\n"
                    f"🗂️ **Posts removed:** {cleared_count}\n"
                    f"🕐 **Time:** {get_ist_time().strftime('%H:%M:%S IST')}"
                )
            else:
                await callback_query.message.edit_text("❌ **No active channel set!**\n\nUse `/setchannel @channel` first.")
        
        await callback_query.answer()
    
    async def handle_message(self, client: Client, message: Message):
        """Handle regular messages (posts to be scheduled)"""
        if message.from_user.id != ADMIN_USER_ID:
            await message.reply_text("❌ **Unauthorized access!**")
            return
        
        # Check if user has set a channel
        current_channel = await self.db.get_current_channel(message.from_user.id)
        if not current_channel:
            await message.reply_text(
                "❌ **No channel set!**\n\n"
                "Please set a channel first using:\n"
                "`/setchannel @yourchannel`"
            )
            return
        
        # Handle media groups (albums)
        if message.media_group_id:
            await self.handle_media_group(client, message)
        else:
            await self.schedule_single_message(client, message)
    
    async def handle_media_group(self, client: Client, message: Message):
        """Handle media group messages (albums)"""
        media_group_id = message.media_group_id
        user_id = message.from_user.id
        
        # Initialize buffer for this user if not exists
        if user_id not in self.media_group_buffer:
            self.media_group_buffer[user_id] = {}
        
        # Add message to buffer
        if media_group_id not in self.media_group_buffer[user_id]:
            self.media_group_buffer[user_id][media_group_id] = []
        
        self.media_group_buffer[user_id][media_group_id].append(message)
        
        # Schedule processing after a delay to collect all items
        await asyncio.sleep(2)
        await self.process_media_group_after_delay(client, user_id, media_group_id)
    
    async def process_media_group_after_delay(self, client: Client, user_id: int, media_group_id: str):
        """Process media group after collecting all items"""
        if user_id not in self.media_group_buffer or media_group_id not in self.media_group_buffer[user_id]:
            return
        
        messages = self.media_group_buffer[user_id][media_group_id]
        del self.media_group_buffer[user_id][media_group_id]
        
        if not messages:
            return
        
        # Get current channel
        current_channel = await self.db.get_current_channel(user_id)
        if not current_channel:
            return
        
        channel_id = current_channel['channel_id']
        
        # Calculate next schedule time
        schedule_time = await self.calculate_next_schedule_time(user_id, channel_id)
        
        # Prepare media group content
        media_list = []
        for msg in messages:
            if msg.photo:
                media_list.append({
                    'type': 'photo',
                    'file_id': msg.photo.file_id,
                    'caption': msg.caption if msg.caption else ""
                })
            elif msg.video:
                media_list.append({
                    'type': 'video',
                    'file_id': msg.video.file_id,
                    'caption': msg.caption if msg.caption else ""
                })
        
        # Store in database
        await self.db.add_scheduled_post(
            user_id=user_id,
            channel_id=channel_id,
            message_type='media_group',
            content={'media_list': media_list},
            scheduled_time=schedule_time
        )
        
        # Update last scheduled time
        if user_id not in self.user_last_scheduled:
            self.user_last_scheduled[user_id] = {}
        self.user_last_scheduled[user_id][channel_id] = schedule_time
        
        # Send confirmation
        first_message = messages[0]
        await first_message.reply_text(
            f"📅 **Album scheduled!**\n\n"
            f"📺 **Channel:** {current_channel['channel_name']}\n"
            f"🖼️ **Items:** {len(media_list)} media files\n"
            f"⏰ **Scheduled for:** {format_ist_time(schedule_time)}\n"
            f"⏳ **In:** {self.get_time_until(schedule_time)}"
        )
    
    async def schedule_single_message(self, client: Client, message: Message):
        """Schedule a single message"""
        current_channel = await self.db.get_current_channel(message.from_user.id)
        channel_id = current_channel['channel_id']
        
        # Calculate next schedule time
        schedule_time = await self.calculate_next_schedule_time(message.from_user.id, channel_id)
        
        # Determine message type and prepare content
        content = {}
        message_type = ""
        
        if message.photo:
            message_type = "photo"
            content = {
                'file_id': message.photo.file_id,
                'caption': message.caption if message.caption else ""
            }
        elif message.video:
            message_type = "video"
            content = {
                'file_id': message.video.file_id,
                'caption': message.caption if message.caption else ""
            }
        elif message.document:
            message_type = "document"
            content = {
                'file_id': message.document.file_id,
                'caption': message.caption if message.caption else "",
                'file_name': message.document.file_name
            }
        elif message.text:
            message_type = "text"
            content = {'text': message.text}
        else:
            await message.reply_text("❌ **Unsupported message type!**")
            return
        
        # Store in database
        await self.db.add_scheduled_post(
            user_id=message.from_user.id,
            channel_id=channel_id,
            message_type=message_type,
            content=content,
            scheduled_time=schedule_time
        )
        
        # Update last scheduled time
        if message.from_user.id not in self.user_last_scheduled:
            self.user_last_scheduled[message.from_user.id] = {}
        self.user_last_scheduled[message.from_user.id][channel_id] = schedule_time
        
        # Send confirmation
        await message.reply_text(
            f"📅 **Post scheduled!**\n\n"
            f"📺 **Channel:** {current_channel['channel_name']}\n"
            f"📝 **Type:** {message_type.title()}\n"
            f"⏰ **Scheduled for:** {format_ist_time(schedule_time)}\n"
            f"⏳ **In:** {self.get_time_until(schedule_time)}"
        )
    
    def get_time_until(self, target_time):
        """Get human readable time until target"""
        now = datetime.utcnow()
        if target_time <= now:
            return "Now"
        
        diff = target_time - now
        hours = diff.seconds // 3600
        minutes = (diff.seconds % 3600) // 60
        
        if hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"
    
    async def show_status(self, message: Message):
        """Show current status"""
        current_channel = await self.db.get_current_channel(message.from_user.id)
        
        if not current_channel:
            await message.reply_text("❌ **No active channel set!**\n\nUse `/setchannel @channel` first.")
            return
        
        stats = await self.db.get_post_stats(message.from_user.id, current_channel['channel_id'])
        
        status_text = f"📊 **Scheduler Status**\n\n"
        status_text += f"📺 **Active Channel:** {current_channel['channel_name']}\n"
        status_text += f"🆔 **Channel ID:** `{current_channel['channel_id']}`\n"
        status_text += f"📝 **Pending Posts:** {stats['pending']}\n"
        status_text += f"✅ **Sent Posts:** {stats['sent']}\n"
        
        if stats['next_scheduled']:
            status_text += f"⏰ **Next Post:** {format_ist_time(stats['next_scheduled'])}\n"
            status_text += f"⏳ **In:** {self.get_time_until(stats['next_scheduled'])}\n"
        else:
            status_text += f"⏰ **Next Post:** No posts scheduled\n"
        
        status_text += f"\n🕐 **Current Time:** {get_ist_time().strftime('%H:%M:%S IST')}"
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="status"),
                InlineKeyboardButton("📺 Channels", callback_data="channels")
            ]
        ])
        
        await message.reply_text(status_text, reply_markup=keyboard)
    
    async def show_channels(self, message: Message):
        """Show all configured channels"""
        channels = await self.db.get_user_channels(message.from_user.id)
        
        if not channels:
            await message.reply_text(
                "📺 **No channels configured!**\n\n"
                "Use `/setchannel @yourchannel` to add a channel."
            )
            return
        
        text = "📺 **Your Channels:**\n\n"
        
        for i, channel in enumerate(channels, 1):
            stats = await self.db.get_post_stats(message.from_user.id, channel['channel_id'])
            current_indicator = "🔸" if channel.get('is_current', False) else "📺"
            
            text += f"{current_indicator} **{channel['channel_name']}**\n"
            text += f"🆔 ID: `{channel['channel_id']}`\n"
            text += f"📝 Pending: {stats['pending']} | ✅ Sent: {stats['sent']}\n"
            
            if stats['next_scheduled']:
                text += f"⏰ Next: {format_ist_time(stats['next_scheduled'])}\n"
            
            text += "\n"
        
        text += "🔸 = Current active channel\n"
        text += f"🕐 **Updated:** {get_ist_time().strftime('%H:%M:%S IST')}"
        
        await message.reply_text(text)
    
    async def show_help(self, message: Message):
        """Show help information"""
        help_text = """❓ **Help & Commands**

**🔧 Setup Commands:**

• `/start` - Start the bot and see main menu
• `/setchannel @channel` - Set/switch target channel for posting
• `/status` - Check scheduling status for active channel
• `/channels` - View all configured channels
• `/clear` - Clear pending posts for active channel
• `/deletechannel CHANNEL_ID` - Permanently delete channel and all its data
• `/help` - Show this help message

**📝 How to Use:**

1. Set your channel with `/setchannel @yourchannel`
2. Forward any post to me (photos, videos, albums, text)
3. I'll automatically schedule them with 1-3 hour intervals
4. Posts will be sent to your active channel automatically
5. Switch channels anytime with `/setchannel`
6. Delete unwanted channels with `/deletechannel CHANNEL_ID`

**⏰ Scheduling Logic:**

• **Interval:** 1-3 hours between posts (randomized)
• **Timezone:** All times shown in IST
• **Queue:** Each channel has independent scheduling
• **Albums:** Handled as single posts to prevent spam

**🎯 Supported Content:**

• 📷 **Photos** - With or without captions
• 🎥 **Videos** - With or without captions  
• 🖼️ **Albums** - Multiple photos/videos as one post
• 📄 **Documents** - Files with captions
• 📝 **Text** - Plain text messages

**⚙️ Channel Requirements:**

• Bot must be **admin** in the channel
• Bot needs **post permissions**
• Channel can be public or private
• Use @ before channel username when setting

**📊 Features:**

• **Multi-channel support** - Switch between channels
• **Queue management** - Clear pending posts anytime  
• **Statistics tracking** - Monitor sent/pending posts
• **Random timing** - Avoid spam detection
• **IST timezone** - All times in Indian Standard Time

**🔐 Security:**

• **Admin only** - Only authorized user can use
• **Channel verification** - Validates permissions
• **Safe scheduling** - Won't post to unauthorized channels

**Need help?** All commands work with buttons or text commands!"""
        
        await message.reply_text(help_text)
    
    async def calculate_next_schedule_time(self, user_id: int, channel_id: str) -> datetime:
        """Calculate next schedule time with 1-3 hour interval"""
        # Get last scheduled time for this channel
        last_time = None
        if user_id in self.user_last_scheduled and channel_id in self.user_last_scheduled[user_id]:
            last_time = self.user_last_scheduled[user_id][channel_id]
        
        # If no last time, use current time
        if not last_time:
            last_time = datetime.utcnow()
        
        # Add random interval between 1-3 hours
        hours_to_add = random.uniform(1, 3)
        next_time = last_time + timedelta(hours=hours_to_add)
        
        # Make sure it's not in the past
        current_time = datetime.utcnow()
        if next_time <= current_time:
            next_time = current_time + timedelta(minutes=random.randint(1, 30))
        
        return next_time
    
    async def send_scheduled_posts(self):
        """Send posts that are scheduled for now"""
        pending_posts = await self.db.get_pending_posts()
        
        for post in pending_posts:
            try:
                channel_id = int(post['channel_id'])
                content = post['content']
                message_type = post['message_type']
                
                if message_type == 'photo':
                    await self.app.send_photo(
                        channel_id,
                        content['file_id'],
                        caption=content.get('caption', '')
                    )
                elif message_type == 'video':
                    await self.app.send_video(
                        channel_id,
                        content['file_id'],
                        caption=content.get('caption', '')
                    )
                elif message_type == 'document':
                    await self.app.send_document(
                        channel_id,
                        content['file_id'],
                        caption=content.get('caption', '')
                    )
                elif message_type == 'text':
                    await self.app.send_message(channel_id, content['text'])
                elif message_type == 'media_group':
                    from pyrogram.types import InputMediaPhoto, InputMediaVideo
                    
                    media_list = []
                    for item in content['media_list']:
                        if item['type'] == 'photo':
                            media_list.append(InputMediaPhoto(
                                item['file_id'],
                                caption=item.get('caption', '')
                            ))
                        elif item['type'] == 'video':
                            media_list.append(InputMediaVideo(
                                item['file_id'],
                                caption=item.get('caption', '')
                            ))
                    
                    if media_list:
                        await self.app.send_media_group(channel_id, media_list)
                
                # Mark as sent
                await self.db.mark_post_sent(str(post['_id']))
                logger.info(f"Sent scheduled post: {post['_id']}")
                
            except Exception as e:
                logger.error(f"Error sending scheduled post {post['_id']}: {e}")
    
    async def start_scheduler(self):
        """Start the background scheduler"""
        while True:
            try:
                await self.send_scheduled_posts()
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            
            # Wait for 60 seconds before checking again
            await asyncio.sleep(60)
    
    async def run(self):
        """Run the bot"""
        # Test database connection
        if not await self.db.test_connection():
            logger.error("Database connection failed!")
            return
        
        # Start health server
        health_server = HealthServer()
        await health_server.start_server()
        
        # Start bot
        await self.app.start()
        logger.info("Bot started successfully")
        
        # Start scheduler in background
        asyncio.create_task(self.start_scheduler())
        
        # Keep the bot running
        await asyncio.Event().wait()

async def main():
    """Main function"""
    # Validate environment variables
    required_vars = ['API_ID', 'API_HASH', 'BOT_TOKEN', 'ADMIN_USER_ID', 'MONGODB_URI']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return
    
    # Start bot
    bot = TelegramSchedulerBot()
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
