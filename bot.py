        
        # Start Telegram bot
        await self.app.start()
        logger.info("Telegram bot started successfully!")
        
        # Start scheduler
        asyncio.create_task(self.start_scheduler())
        
        # Keep running
        await asyncio.Event().wait()

async def main():
    """Main function"""
    if not all([API_ID, API_HASH, BOT_TOKEN, MONGODB_URI, ADMIN_USER_ID]):
        logger.error("Missing required environment variables!")
        return
    
    bot = TelegramSchedulerBot()
    await bot.run()

if __name__ == '__main__':
    asyncio.run(main())
