import asyncio
import logging
from datetime import datetime
from typing import Dict, List
import pytz
from pyrogram import Client
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
from database import Database

IST = pytz.timezone('Asia/Kolkata')
logger = logging.getLogger(__name__)

class PostScheduler:
    def __init__(self, client: Client, database: Database):
        self.client = client
        self.db = database
        self.running = False
    
    async def start(self):
        """Start the scheduler"""
        self.running = True
        logger.info("Scheduler started")
        
        while self.running:
            try:
                await self.process_pending_posts()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Scheduler error: {str(e)}")
                await asyncio.sleep(60)
    
    async def process_pending_posts(self):
        """Process all pending posts that are ready to be sent"""
        pending_posts = await self.db.get_pending_posts()
        
        # Group posts by media_group_id for albums
        media_groups = {}
        single_posts = []
        
        for post in pending_posts:
            if post.get('media_group_id'):
                group_id = post['media_group_id']
                if group_id not in media_groups:
                    media_groups[group_id] = []
                media_groups[group_id].append(post)
            else:
                single_posts.append(post)
        
        # Send media groups (albums)
        for group_id, posts in media_groups.items():
            await self.send_media_group(posts)
        
        # Send single posts
        for post in single_posts:
            await self.send_single_post(post)
    
    async def send_media_group(self, posts: List[Dict]):
        """Send a media group (album)"""
        try:
            if not posts:
                return
            
            channel_id = posts[0]['channel_id']
            media = []
            
            for post in posts:
                if post['message_type'] == 'photo':
                    media.append(InputMediaPhoto(
                        media=post['file_id'],
                        caption=post.get('caption', '') if len(media) == 0 else None
                    ))
                elif post['message_type'] == 'video':
                    media.append(InputMediaVideo(
                        media=post['file_id'],
                        caption=post.get('caption', '') if len(media) == 0 else None
                    ))
                elif post['message_type'] == 'document':
                    media.append(InputMediaDocument(
                        media=post['file_id'],
                        caption=post.get('caption', '') if len(media) == 0 else None
                    ))
            
            if media:
                await self.client.send_media_group(
                    chat_id=channel_id,
                    media=media
                )
                
                # Update all posts in the group as sent
                for post in posts:
                    await self.db.update_post_status(str(post['_id']), 'sent')
                
                logger.info(f"Sent media group to {channel_id} with {len(posts)} items")
        
        except Exception as e:
            logger.error(f"Error sending media group: {str(e)}")
            # Mark posts as failed
            for post in posts:
                await self.db.update_post_status(str(post['_id']), 'failed')
    
    async def send_single_post(self, post: Dict):
        """Send a single post"""
        try:
            channel_id = post['channel_id']
            
            if post['message_type'] == 'photo':
                await self.client.send_photo(
                    chat_id=channel_id,
                    photo=post['file_id'],
                    caption=post.get('caption')
                )
            elif post['message_type'] == 'video':
                await self.client.send_video(
                    chat_id=channel_id,
                    video=post['file_id'],
                    caption=post.get('caption')
                )
            elif post['message_type'] == 'document':
                await self.client.send_document(
                    chat_id=channel_id,
                    document=post['file_id'],
                    caption=post.get('caption')
                )
            elif post['message_type'] == 'text':
                await self.client.send_message(
                    chat_id=channel_id,
                    text=post['text']
                )
            
            # Update post as sent
            await self.db.update_post_status(str(post['_id']), 'sent')
            logger.info(f"Sent single post to {channel_id}")
        
        except Exception as e:
            logger.error(f"Error sending single post: {str(e)}")
            await self.db.update_post_status(str(post['_id']), 'failed')
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        logger.info("Scheduler stopped")
