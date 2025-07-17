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
BOT_TOKEN = os.getenv('BOT_TOKEN', '7984590797:AAGa9XAQg-FoXNG7-lfSHixHrISdtAChMMU')
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

# Timezone setup
IST = pytz.timezone('Asia/Kolkata')

def get_ist_time():
    return datetime.now(IST)

class HealthServer:
    def __init__(self, port: int = PORT):
        self.port = port
        self.app = web.Application()
        self.app.router.add_get('/', self.health_check)
        self.runner = None
        self.site = None

    async def health_check(self, request):
        return web.json_response({
            "status": "healthy",
            "timestamp": get_ist_time().isoformat(),
            "message": "Telegram Scheduler Bot is running"
        })

    async def start_server(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
        await self.site.start()

    async def stop_server(self):
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

class MongoDBManager:
    def __init__(self, uri: str):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client.telegram_scheduler
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

    async def save_scheduled_post(self, post_data: Dict[str, Any]):
        try:
            result = await self.scheduled_posts.insert_one(post_data)
            logger.info(f"Scheduled post saved with ID: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"Error saving scheduled post: {e}")
            return None

    async def get_pending_posts(self, user_id: int) -> List[Dict]:
        try:
            cursor = self.scheduled_posts.find({
                'user_id': user_id,
                'status': 'pending',
                'scheduled_time': {'$lte': get_ist_time()}
            }).sort('scheduled_time', 1)
            
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error fetching pending posts: {e}")
            return []

    async def update_post_status(self, post_id: str, status: str, error_message: str = None):
        try:
            update_data = {
                'status': status,
                'processed_at': get_ist_time()
            }
            if error_message:
                update_data['error_message'] = error_message
            
            await self.scheduled_posts.update_one(
                {'_id': post_id},
                {'$set': update_data}
            )
            logger.info(f"Post {post_id} status updated to: {status}")
        except Exception as e:
            logger.error(f"Error updating post status: {e}")

    async def get_current_channel(self, user_id: int) -> Optional[Dict]:
        try:
            return await self.bot_settings.find_one({
                'user_id': user_id,
                'setting_type': 'current_channel'
            })
        except Exception as e:
            logger.error(f"Error getting current channel: {e}")
            return None

    async def set_current_channel(self, user_id: int, channel_id: str, channel_name: str):
        try:
            await self.bot_settings.update_one(
                {'user_id': user_id, 'setting_type': 'current_channel'},
                {'$set': {
                    'channel_id': channel_id,
                    'channel_name': channel_name,
                    'updated_at': get_ist_time()
                }},
                upsert=True
            )
            logger.info(f"Current channel set to: {channel_name} ({channel_id})")
        except Exception as e:
            logger.error(f"Error setting current channel: {e}")

    async def get_all_scheduled_posts(self, user_id: int) -> List[Dict]:
        try:
            cursor = self.scheduled_posts.find({'user_id': user_id}).sort('scheduled_time', 1)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error fetching all scheduled posts: {e}")
            return []

    async def delete_scheduled_post(self, post_id: str):
        try:
            result = await self.scheduled_posts.delete_one({'_id': post_id})
            if result.deleted_count > 0:
                logger.info(f"Scheduled post {post_id} deleted successfully")
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting scheduled post: {e}")
            return False

    async def delete_channel_data_if_last_post_sent(self, user_id: int, channel_id: str):
        """Delete all data of the channel if last post has been posted"""
        try:
            pending_count = await self.scheduled_posts.count_documents({
                'user_id': user_id,
                'channel_id': channel_id,
                'status': 'pending'
            })
            
            if pending_count == 0:
                # Delete all posts for this channel
                await self.scheduled_posts.delete_many({
                    'user_id': user_id, 
                    'channel_id': channel_id
                })
                
                # Delete channel settings
                await self.bot_settings.delete_many({
                    'user_id': user_id, 
                    'channel_id': channel_id
                })
                
                logger.info(f"Deleted all data for channel {channel_id} as last post was sent")
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error deleting channel data: {e}")
            return False

    async def close(self):
        self.client.close()

class TelegramSchedulerBot:
    def __init__(self):
        self.app = Client(
            "scheduler_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN
        )
        self.db = MongoDBManager(MONGODB_URI)
        self.health_server = HealthServer()
        self.is_running = False
        self.setup_handlers()

    def setup_handlers(self):
        @self.app.on_message(filters.command("start") & filters.user(ADMIN_USER_ID))
        async def start_command(client, message: Message):
            await message.reply_text(
                "🚀 **Telegram Scheduler Bot Started!**\n\n"
                "**Available Commands:**\n"
                "📝 `/schedule` - Schedule a new post\n"
                "📋 `/list` - View all scheduled posts\n"
                "🗑️ `/delete` - Delete a scheduled post\n"
                "⚙️ `/setchannel` - Set target channel\n"
                "📊 `/status` - Check bot status\n"
                "❓ `/help` - Show this help message\n\n"
                "**Bot is ready to schedule your posts!** ✨"
            )

        @self.app.on_message(filters.command("help") & filters.user(ADMIN_USER_ID))
        async def help_command(client, message: Message):
            await message.reply_text(
                "📚 **Help - Telegram Scheduler Bot**\n\n"
                "**Commands:**\n"
                "• `/start` - Initialize the bot\n"
                "• `/schedule` - Schedule a new post\n"
                "• `/list` - View all scheduled posts\n"
                "• `/delete [post_id]` - Delete a specific post\n"
                "• `/setchannel [channel_id]` - Set target channel\n"
                "• `/status` - Check bot and database status\n"
                "• `/help` - Show this help message\n\n"
                "**Usage Examples:**\n"
                "• `/schedule Hello World | 2024-01-01 10:30`\n"
                "• `/setchannel @mychannel`\n"
                "• `/delete 507f1f77bcf86cd799439011`\n\n"
                "**Time Format:** YYYY-MM-DD HH:MM (24-hour format)\n"
                "**Timezone:** Asia/Kolkata (IST)"
            )

        @self.app.on_message(filters.command("status") & filters.user(ADMIN_USER_ID))
        async def status_command(client, message: Message):
            try:
                db_status = await self.db.test_connection()
                current_channel = await self.db.get_current_channel(ADMIN_USER_ID)
                pending_posts = await self.db.get_pending_posts(ADMIN_USER_ID)
                
                channel_info = f"📺 **Channel:** {current_channel['channel_name']}" if current_channel else "❌ **No channel set**"
                
                status_text = (
                    f"📊 **Bot Status Report**\n\n"
                    f"🤖 **Bot:** {'✅ Running' if self.is_running else '❌ Stopped'}\n"
                    f"🗄️ **Database:** {'✅ Connected' if db_status else '❌ Disconnected'}\n"
                    f"{channel_info}\n"
                    f"⏰ **Pending Posts:** {len(pending_posts)}\n"
                    f"🕐 **Current Time:** {get_ist_time().strftime('%Y-%m-%d %H:%M:%S IST')}"
                )
                
                await message.reply_text(status_text)
            except Exception as e:
                await message.reply_text(f"❌ Error checking status: {str(e)}")

        @self.app.on_message(filters.command("setchannel") & filters.user(ADMIN_USER_ID))
        async def set_channel_command(client, message: Message):
            try:
                command_parts = message.text.split(maxsplit=1)
                if len(command_parts) < 2:
                    await message.reply_text("❌ Please provide a channel ID or username.\n\n**Usage:** `/setchannel @channel_username`")
                    return
                
                channel_id = command_parts[1].strip()
                
                # Test if channel exists and bot has access
                try:
                    chat = await client.get_chat(channel_id)
                    await self.db.set_current_channel(ADMIN_USER_ID, str(chat.id), chat.title or channel_id)
                    await message.reply_text(f"✅ Channel set successfully!\n\n**Channel:** {chat.title or channel_id}")
                except Exception as e:
                    await message.reply_text(f"❌ Error accessing channel: {str(e)}\n\nMake sure the bot is added to the channel as an admin.")
                    
            except Exception as e:
                await message.reply_text(f"❌ Error setting channel: {str(e)}")

        @self.app.on_message(filters.command("schedule") & filters.user(ADMIN_USER_ID))
        async def schedule_command(client, message: Message):
            try:
                current_channel = await self.db.get_current_channel(ADMIN_USER_ID)
                if not current_channel:
                    await message.reply_text("❌ No channel set. Use `/setchannel` first.")
                    return
                
                command_parts = message.text.split(maxsplit=1)
                if len(command_parts) < 2:
                    await message.reply_text(
                        "❌ Please provide message and time.\n\n"
                        "**Usage:** `/schedule Your message here | 2024-01-01 10:30`\n"
                        "**Time Format:** YYYY-MM-DD HH:MM (24-hour format)"
                    )
                    return
                
                content = command_parts[1].strip()
                if '|' not in content:
                    await message.reply_text("❌ Please separate message and time with '|'")
                    return
                
                message_text, time_str = content.split('|', 1)
                message_text = message_text.strip()
                time_str = time_str.strip()
                
                # Parse time
                scheduled_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M')
                scheduled_time = IST.localize(scheduled_time)
                
                if scheduled_time <= get_ist_time():
                    await message.reply_text("❌ Scheduled time must be in the future.")
                    return
                
                # Save to database
                post_data = {
                    'user_id': ADMIN_USER_ID,
                    'channel_id': current_channel['channel_id'],
                    'channel_name': current_channel['channel_name'],
                    'message_text': message_text,
                    'scheduled_time': scheduled_time,
                    'status': 'pending',
                    'created_at': get_ist_time()
                }
                
                post_id = await self.db.save_scheduled_post(post_data)
                if post_id:
                    await message.reply_text(
                        f"✅ **Post scheduled successfully!**\n\n"
                        f"**Channel:** {current_channel['channel_name']}\n"
                        f"**Time:** {scheduled_time.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
                        f"**Message:** {message_text[:100]}{'...' if len(message_text) > 100 else ''}\n"
                        f"**Post ID:** `{post_id}`"
                    )
                else:
                    await message.reply_text("❌ Failed to schedule post. Please try again.")
                    
            except ValueError:
                await message.reply_text("❌ Invalid time format. Use: YYYY-MM-DD HH:MM")
            except Exception as e:
                await message.reply_text(f"❌ Error scheduling post: {str(e)}")

        @self.app.on_message(filters.command("list") & filters.user(ADMIN_USER_ID))
        async def list_command(client, message: Message):
            try:
                posts = await self.db.get_all_scheduled_posts(ADMIN_USER_ID)
                
                if not posts:
                    await message.reply_text("📭 No scheduled posts found.")
                    return
                
                response = "📋 **Scheduled Posts:**\n\n"
                for post in posts:
                    status_emoji = "⏰" if post['status'] == 'pending' else "✅" if post['status'] == 'sent' else "❌"
                    
                    response += (
                        f"{status_emoji} **ID:** `{post['_id']}`\n"
                        f"📺 **Channel:** {post['channel_name']}\n"
                        f"🕐 **Time:** {post['scheduled_time'].strftime('%Y-%m-%d %H:%M:%S IST')}\n"
                        f"📝 **Message:** {post['message_text'][:50]}{'...' if len(post['message_text']) > 50 else ''}\n"
                        f"📊 **Status:** {post['status'].title()}\n\n"
                    )
                
                # Split long messages
                if len(response) > 4000:
                    for i in range(0, len(response), 4000):
                        await message.reply_text(response[i:i+4000])
                else:
                    await message.reply_text(response)
                    
            except Exception as e:
                await message.reply_text(f"❌ Error fetching posts: {str(e)}")

        @self.app.on_message(filters.command("delete") & filters.user(ADMIN_USER_ID))
        async def delete_command(client, message: Message):
            try:
                command_parts = message.text.split(maxsplit=1)
                if len(command_parts) < 2:
                    await message.reply_text("❌ Please provide a post ID.\n\n**Usage:** `/delete [post_id]`")
                    return
                
                post_id = command_parts[1].strip()
                
                success = await self.db.delete_scheduled_post(post_id)
                if success:
                    await message.reply_text(f"✅ Post deleted successfully!\n\n**Post ID:** `{post_id}`")
                else:
                    await message.reply_text(f"❌ Post not found or already deleted.\n\n**Post ID:** `{post_id}`")
                    
            except Exception as e:
                await message.reply_text(f"❌ Error deleting post: {str(e)}")

    async def send_scheduled_post(self, post_data: Dict):
        try:
            await self.app.send_message(
                chat_id=post_data['channel_id'],
                text=post_data['message_text']
            )
            
            await self.db.update_post_status(post_data['_id'], 'sent')
            logger.info(f"Posted message to {post_data['channel_name']}")
            
            # Check if this was the last post and delete channel data
            await self.db.delete_channel_data_if_last_post_sent(
                post_data['user_id'],
                post_data['channel_id']
            )
            
        except FloodWait as e:
            logger.warning(f"FloodWait: {e.value} seconds")
            await asyncio.sleep(e.value)
            await self.send_scheduled_post(post_data)
        except (UserIsBlocked, PeerIdInvalid) as e:
            error_msg = f"Channel access error: {str(e)}"
            await self.db.update_post_status(post_data['_id'], 'failed', error_msg)
            logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error sending post: {str(e)}"
            await self.db.update_post_status(post_data['_id'], 'failed', error_msg)
            logger.error(error_msg)

    async def start_scheduler(self):
        logger.info("Scheduler started")
        while self.is_running:
            try:
                pending_posts = await self.db.get_pending_posts(ADMIN_USER_ID)
                
                for post in pending_posts:
                    await self.send_scheduled_post(post)
                    await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            
            await asyncio.sleep(60)  # Check every minute

    async def auto_set_commands(self):
        """Auto-set bot commands on start"""
        try:
            commands = [
                ("start", "Initialize the bot"),
                ("help", "Show help message"),
                ("schedule", "Schedule a new post"),
                ("list", "View all scheduled posts"),
                ("delete", "Delete a scheduled post"),
                ("setchannel", "Set target channel"),
                ("status", "Check bot status")
            ]
            
            await self.app.set_bot_commands(commands)
            logger.info("Bot commands set successfully")
            
        except Exception as e:
            logger.error(f"Error setting bot commands: {e}")

    async def run(self):
        """Run the bot with health server"""
        try:
            # Test MongoDB connection
            if not await self.db.test_connection():
                logger.error("Failed to connect to MongoDB. Exiting...")
                return

            # AUTO CLEANUP: Delete channel data if last post has been sent
            current_channel = await self.db.get_current_channel(ADMIN_USER_ID)
            if current_channel:
                deleted = await self.db.delete_channel_data_if_last_post_sent(
                    ADMIN_USER_ID, 
                    current_channel['channel_id']
                )
                if deleted:
                    logger.info(f"Auto-deleted data for channel {current_channel['channel_id']} on bot start")

            # Start health server
            await self.health_server.start_server()
            logger.info(f"Health server started on port {PORT}")

            # Start Telegram bot
            await self.app.start()
            logger.info(f"Telegram bot started successfully at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S IST')}")

            # Auto-set bot commands
            await self.auto_set_commands()

            # Set running flag
            self.is_running = True

            # Start scheduler
            asyncio.create_task(self.start_scheduler())

            # Keep running
            await asyncio.Event().wait()

        except Exception as e:
            logger.error(f"Error starting bot: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        await self.health_server.stop_server()
        await self.db.close()
        await self.app.stop()

if __name__ == "__main__":
    bot = TelegramSchedulerBot()
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
