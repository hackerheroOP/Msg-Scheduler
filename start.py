import asyncio
from aiohttp import web
import main

async def health_check(request):
    return web.Response(text='OK')

async def start_web_server():
    """Start web server without signal handlers"""
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', 8000)
    await site.start()
    print("Health check server started on port 8000")

async def main_async():
    """Main async function to run both services"""
    # Start web server
    await start_web_server()
    
    # Start the bot (this will run indefinitely)
    await main.main()

if __name__ == "__main__":
    asyncio.run(main_async())
