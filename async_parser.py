import aiohttp
from aiohttp_proxy import ProxyConnector, ProxyType
import asyncio
import sys
import numpy as np

if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def fetch(url, proxy):
    host, port = proxy.split(':')[0], proxy.split(':')[1]
    connector = ProxyConnector(
         proxy_type=ProxyType.HTTP,
         host=host,
         port= int(port),
    )
    async with aiohttp.ClientSession(connector=connector,trust_env=True) as session:
        params = {
            'text': f'NAME:C++',
            'area': 113,
            'page': 0,
            'per_page': 100
        }
        async with session.get(url, params=params) as response:
            return await response.text()

if __name__ == "__main__":
    data = np.load('file.npy')
    print(f'we take {data[2]} proxy')
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(fetch('https://api.hh.ru/vacancies', data[2]))
    print(result)
    loop.run_until_complete(asyncio.sleep(0.01))
    loop.close()