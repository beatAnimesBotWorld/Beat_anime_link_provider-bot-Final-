"""
Health check endpoint for external monitoring
"""
from aiohttp import web
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class HealthCheckServer:
    def __init__(self, port=8080):
        self.port = port
        self.app = web.Application()
        self.runner = None
        self.site = None
        self.last_activity = datetime.now()
        
        # Routes
        self.app.router.add_get('/', self.health_check)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/ping', self.ping)
    
    async def health_check(self, request):
        return web.Response(text='OK', status=200)
    
    async def ping(self, request):
        self.last_activity = datetime.now()
        return web.json_response({
            'status': 'alive',
            'timestamp': self.last_activity.isoformat()
        })
    
    async def start(self):
        try:
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            logger.info(f"✅ Health check server running on port {self.port}")
        except Exception as e:
            logger.error(f"❌ Failed to start health server: {e}")
    
    async def stop(self):
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

health_server = HealthCheckServer()
