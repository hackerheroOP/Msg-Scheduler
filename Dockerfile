FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create a simple web server for health checks (required by Koyeb)
EXPOSE 8000

# Start both web server and bot
CMD python -c "
import asyncio
import threading
from aiohttp import web
import main

# Simple health check server
async def health_check(request):
    return web.Response(text='OK')

app = web.Application()
app.router.add_get('/', health_check)
app.router.add_get('/health', health_check)

# Run web server in thread
def run_web_server():
    web.run_app(app, host='0.0.0.0', port=8000)

# Start web server in background thread
web_thread = threading.Thread(target=run_web_server)
web_thread.daemon = True
web_thread.start()

# Run the main bot
asyncio.run(main.main())
"
