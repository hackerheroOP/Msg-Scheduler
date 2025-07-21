import motor.motor_asyncio
from datetime import datetime
import pytz
from typing import List, Dict, Optional

IST = pytz.timezone('Asia/Kolkata')

class Database:
    def __init__(self, mongodb_uri: str):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_uri)
        self.db = self.client.scheduler_bot
        self.posts = self.db.posts
    
    async def save_post(self, post_data: Dict) -> str:
        """Save a post to database"""
        result = await self.posts.insert_one(post_data)
        return str(result.inserted_id)
    
    async def get_pending_posts(self) -> List[Dict]:
        """Get all pending posts that are ready to be sent"""
        current_time = datetime.now(IST)
        cursor = self.posts.find({
            'status': 'pending',
            'scheduled_time': {'$lte': current_time}
        }).sort('scheduled_time', 1)
        
        return await cursor.to_list(length=None)
    
    async def get_last_post(self, channel_id: str) -> Optional[Dict]:
        """Get the last scheduled post for a channel"""
        cursor = self.posts.find({
            'channel_id': channel_id
        }).sort('scheduled_time', -1).limit(1)
        
        posts = await cursor.to_list(length=1)
        return posts[0] if posts else None
    
    async def update_post_status(self, post_id: str, status: str) -> bool:
        """Update post status"""
        from bson import ObjectId
        result = await self.posts.update_one(
            {'_id': ObjectId(post_id)},
            {'$set': {'status': status, 'sent_at': datetime.now(IST)}}
        )
        return result.modified_count > 0
    
    async def delete_channel_posts(self, channel_id: str) -> int:
        """Delete all posts for a specific channel"""
        result = await self.posts.delete_many({'channel_id': channel_id})
        return result.deleted_count
    
    async def empty_database(self) -> int:
        """Clear entire database"""
        result = await self.posts.delete_many({})
        return result.deleted_count
    
    async def get_pending_posts_count(self, channel_id: str) -> int:
        """Get count of pending posts for a channel"""
        count = await self.posts.count_documents({
            'channel_id': channel_id,
            'status': 'pending'
        })
        return count
