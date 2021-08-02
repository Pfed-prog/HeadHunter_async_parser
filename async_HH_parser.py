import aiohttp
import asyncio
import time
import numpy as np
import pandas as pd
import functools
import operator
import argparse
import re
from datetime import datetime

async def get_page(page: int, search_text: str ) -> list:
    async with aiohttp.ClientSession(trust_env=True) as client:
        vac_list = []
        for x in set(np.arange(page)):
            params = {
                'text': f'NAME:{search_text}',
                'area': 113, #113--Russia; 1001--Other Regions(outside CIS)
                'page': int(x),
                'per_page': 100
            }
            async with client.get('https://api.hh.ru/vacancies', params=params) as response:
                data = await response.json()
                vac_list.append([data['items'][i]['url'] for i in range(len(data['items']))])
                if (data['pages'] - x) <= 1:
                    print('The maximum number of vacancies:')
                    break
        return vac_list

async def get_vacancy(url: str) -> list:
    async with aiohttp.ClientSession(trust_env=True) as client:
        async with client.get(f"{url}", raise_for_status=True) as response:
            response_dict = await response.json()
            name = response_dict['name']
            skills: str = ''
            if 'key_skills' in response_dict:
                for i in range(len(response_dict['key_skills'])):
                    skills = skills + response_dict['key_skills'][i]['name']+';'
            sal_from: str = ''
            sal_to: str   = ''
            sal_cur: str  = ''
            if 'salary' in response_dict:
                if not (response_dict['salary'] is None):
                    sal_from = response_dict['salary']['from']
                    sal_to   = response_dict['salary']['to']
                    sal_cur  = response_dict['salary']['currency']

            exp = response_dict['experience']['name']
            sch = response_dict['schedule']['name']
            employer = response_dict['employer']['name']
            description =  re.sub('<[^<]+?>', '', response_dict['description'])
            area = response_dict['area']['name']
            proper_url = response_dict['apply_alternate_url']
            published = response_dict['published_at']
            test = response_dict['has_test']
            
    return [name,  proper_url, published, test, sal_from, sal_to, sal_cur, exp,  sch, skills, employer, area, description]

class Limiter:
    def __init__(self, delay):
        self.delay = delay
        self._ready = asyncio.Event()
        self._ready.set()

    async def wait(self):
        while not self._ready.is_set():
            await self._ready.wait()
        self._ready.clear()
        asyncio.get_event_loop().call_later(self.delay, self._ready.set)

async def try_make_request(limiter, x):
    while True:
        await limiter.wait()
        return await get_vacancy(x)

async def main(pages, text, delay=0.2):
    vac_list = await asyncio.gather(get_page(pages, text))
    vac_list = functools.reduce(operator.iconcat, vac_list[0], [])
    print(len(vac_list))
    limiter = Limiter(delay)
    tasks = {asyncio.ensure_future(try_make_request(limiter, c)): c for c in vac_list}
    pending = set(tasks.keys())
    num_times_called = 0
    while pending:
        num_times_called += 1
        finished, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_EXCEPTION)
        for task in finished:
            if task.exception():
                print(f"{task} got an exception {task.exception()}")
                return [x.result() for x in finished]
                #for retrying
                #coro = tasks[task]
                #new_task = asyncio.ensure_future(get_vacancy(coro))
                #tasks[new_task] = coro
                #pending.add(new_task)
    print("saving to output file")
    return [x.result() for x in finished]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--pages', default=5, dest='pages', type=int, help='provide the number of pages to scrape. 1 page has 100 vacancies.')
    parser.add_argument('-t','--text', default='', dest='search_text', type=str, help='provide the search text.')
    parser.add_argument('-o','--output', default='df', dest='file_name', type=str, help='provide the output file name.')
    parser.add_argument('-d','--delay', default=0.2, dest='delay', type=float, help='provide the delay.')
    args = parser.parse_args()
    s = time.perf_counter()
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(main(args.pages, args.search_text, args.delay))
    pd.DataFrame(np.array(res),columns=['vacancy','url', 'created', 'has_test', 'salary_from','salary_to', 'currency', 'experience', 'schedule','skills', 'employer', 'area', 'description']).to_csv(f'{args.file_name}{datetime.now().date()}.csv',index=False)
    loop.run_until_complete(asyncio.sleep(0.01))
    loop.close()
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")