import asyncio
from aiohttp import web, ClientSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

INFLUX_HOST = 'influxdb'
INFLUX_PORT = 8181

async def handle(request):
    auth = request.headers.get('Authorization','')
    peer = request.remote
    logging.info(f"proxy: received {request.method} {request.rel_url} from {peer} Authorization=" + (auth[:64] if auth else '<none>'))
    data = await request.read()
    url = f'http://{INFLUX_HOST}:{INFLUX_PORT}{request.rel_url}'
    headers = {k:v for k,v in request.headers.items()}
    async with ClientSession() as session:
        async with session.request(request.method, url, headers=headers, data=data) as resp:
            body = await resp.read()
            return web.Response(status=resp.status, body=body, headers=resp.headers)

app = web.Application()
app.router.add_route('*', '/{tail:.*}', handle)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8182)
