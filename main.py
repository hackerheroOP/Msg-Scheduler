import os
import asyncio
import random
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message
import pytz
from database import Database
from scheduler import PostScheduler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bot configuration
API_ID = os.getenv("API_ID", "1560761")
API_HASH = os.getenv("API_HASH", 'd7e3b89b16213382fa173a9c3b5d6cc4')
BOT_TOKEN = os.getenv("BOT_TOKEN", '7984590797:AAEgnVfl6QDWTlTIpB7hWresGiTkmnbMI88' )
MONGODB_URI = os.getenv("MONGODB_URI", 'mongodb+srv://wtflinksofficial:wtflinksofficial@cluster0.1uld4.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')

# Initialize bot
app = Client("scheduler_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Initialize database and scheduler
db = Database(MONGODB_URI)
scheduler = PostScheduler(app, db)

# User sessions to track current channel
user_sessions = {}

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    welcome_text = """
🤖 **Telegram Post Scheduler Bot**

Commands:
• `/setchannel <channel_id>` - Set current channel for posting
• `/delete <channel_id>` - Delete all posts from specific channel
• `/empty` - Clear entire database
• `/status` - Check current channel and pending posts

After setting a channel, simply forward posts to me and I'll schedule them with random intervals (1-3 hours) in IST timezone!
    """
    await message.reply(welcome_text)

@app.on_message(filters.command("setchannel"))
async def set_channel(client, message: Message):
    try:
        channel_id = message.text.split(" ", 1)[1].strip()
        
        # Validate channel ID format
        if not channel_id.startswith("-100"):
            await message.reply("❌ Invalid channel ID format. Use format: -1002818654243")
            return
            
        user_sessions[message.from_user.id] = channel_id
        
        # Check if bot has access to channel
        try:
            chat = await client.get_chat(channel_id)
            await message.reply(f"✅ Channel set successfully!\n**Channel:** {chat.title}\n**ID:** {channel_id}")
        except Exception as e:
            await message.reply(f"⚠️ Channel set but couldn't verify access: {str(e)}")
            
    except IndexError:
        await message.reply("❌ Please provide channel ID.\nUsage: `/setchannel -1002818654243`")
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.command("delete"))
async def delete_channel_data(client, message: Message):
    try:
        channel_id = message.text.split(" ", 1)[1].strip()
        
        deleted_count = await db.delete_channel_posts(channel_id)
        await message.reply(f"✅ Deleted {deleted_count} posts from channel {channel_id}")
        
    except IndexError:
        await message.reply("❌ Please provide channel ID.\nUsage: `/delete -1002818654243`")
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.command("empty"))
async def empty_database(client, message: Message):
    try:
        deleted_count = await db.empty_database()
        await message.reply(f"✅ Database cleared! Deleted {deleted_count} total posts.")
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.command("status"))
async def status_command(client, message: Message):
    try:
        user_id = message.from_user.id
        current_channel = user_sessions.get(user_id, "Not set")
        
        if current_channel != "Not set":
            pending_posts = await db.get_pending_posts_count(current_channel)
            status_text = f"📊 **Status**\n\n**Current Channel:** {current_channel}\n**Pending Posts:** {pending_posts}"
        else:
            status_text = "📊 **Status**\n\n**Current Channel:** Not set\nUse `/setchannel <channel_id>` to set a channel"
            
        await message.reply(status_text)
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.forwarded | (filters.photo | filters.video | filters.document | filters.text))
async def handle_forwarded_post(client, message: Message):
    user_id = message.from_user.id
    
    # Check if user has set a channel
    if user_id not in user_sessions:
        await message.reply("❌ Please set a channel first using `/setchannel <channel_id>`")
        return
    
    channel_id = user_sessions[user_id]
    
    try:
        # Get the last scheduled time for this channel
        last_post = await db.get_last_post(channel_id)
        
        # Calculate random delay (1-3 hours)
        random_hours = random.uniform(1, 2)
        random_minutes = int(random_hours * 30)
        
        # Calculate scheduled time in IST
        if last_post:
            base_time = last_post['scheduled_time']
        else:
            base_time = datetime.now(IST)
        
        scheduled_time = base_time + timedelta(minutes=random_minutes)
        
        # Store post in database
        post_data = {
            'channel_id': channel_id,
            'user_id': user_id,
            'message_type': 'media_group' if message.media_group_id else get_message_type(message),
            'media_group_id': message.media_group_id,
            'scheduled_time': scheduled_time,
            'status': 'pending',
            'created_at': datetime.now(IST)
        }
        
        # Handle different message types
        if message.photo:
            post_data['file_id'] = message.photo.file_id
            post_data['caption'] = message.caption
        elif message.video:
            post_data['file_id'] = message.video.file_id
            post_data['caption'] = message.caption
        elif message.document:
            post_data['file_id'] = message.document.file_id
            post_data['caption'] = message.caption
        elif message.text:
            post_data['text'] = message.text
        
        await db.save_post(post_data)
        
        # Format scheduled time for display
        scheduled_time_str = scheduled_time.strftime("%d/%m/%Y %I:%M %p IST")
        
        await message.reply(f"✅ Post scheduled!\n**Channel:** {channel_id}\n**Scheduled for:** {scheduled_time_str}")
        
    except Exception as e:
        logger.error(f"Error handling post: {str(e)}")
        await message.reply(f"❌ Error scheduling post: {str(e)}")

def get_message_type(message):
    if message.photo:
        return 'photo'
    elif message.video:
        return 'video'
    elif message.document:
        return 'document'
    elif message.text:
        return 'text'
    else:
        return 'other'

async def main():
    # Start the scheduler
    scheduler_task = asyncio.create_task(scheduler.start())
    
    # Start the bot
    await app.start()
    logger.info("Bot started successfully!")
    
    # Keep the bot running
    await asyncio.gather(scheduler_task)

if __name__ == "__main__":
    asyncio.run(main())
