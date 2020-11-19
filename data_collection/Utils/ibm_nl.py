import asyncio
import json
import logging
import sys
from time import sleep
from typing import Dict, List, Tuple

import aiohttp

import ibm_settings


IBM_CREDENTIALS = ibm_settings.IBM_CREDENTIALS
API_KEY = IBM_CREDENTIALS['apikey']
URL = f'{IBM_CREDENTIALS["url"]}/v1/analyze?version=2019-07-12'
HEADERS = {
    'Content-Type': 'application/json',
}
DATA = {
    'text': '',
    "features": {
        "entities": {
            "sentiment": True,
        },
    }
}

logging.basicConfig(level='INFO')

async def request_ibm(text: str, session: aiohttp.ClientSession)->Dict:
    '''
        Sends text to ibm nl entity and sentiment recognition service.
        Returns Dict with response data.
    '''
    data = DATA.copy()
    data['text'] = text
    async with session.post(URL, data=json.dumps(data)) as response:
        response_data = await response.json()
    return response_data

def get_company_data(entity: Dict)->Dict:
    '''
        Retrieves name, sentiment score, sentiment label, relevance,
        number of references, and confidence from IBM entity dict if 
        entity has type 'Company'.
        For 'Company' returns dict:
            {
                'organization': str,
                'sentiment_score': float -1:1,
                'sentiment_lable': 'neutral' | 'positive' | 'negative',
                'sentiment_label': str,
                'org_relevance': float 0:1,
                'org_count': int,
                'confidence': float 0:1
            }
        For othe types returns {}
    '''
    result = dict()
    if entity['type'] == 'Company':
        result['organization'] = entity.get('text')
        sentiment = entity.get('sentiment')
        if sentiment:
            result['sentiment_score'] = sentiment.get('score')
            result['sentiment_label'] = sentiment.get('label')
        result['org_relevance'] = entity.get('relevance')
        result['org_count'] = entity.get('count')
        result['confidence'] = entity.get('confidence')
    return result

async def check_text(text: str, session: aiohttp.ClientSession, uuid: str = '0')->List[Dict[str,str]]:
    '''
        Sends request to IBM entity and sentiment
        recognition service, retrives all organizations
        from response and returns in List.
    '''
    response = await request_ibm(text, session)
    result = []
    counter = 0
    try:
        for entity in response['entities']:
            company_data = get_company_data(entity)
            if company_data:
                counter += 1
                company_data['uuid'] = uuid
                result.append(company_data)
    except KeyError:
        logging.exception(f'No entities in ibm_response:{response}')
    for company in result:
        company['orgs_in_doc'] = counter
    return result

def progress(count:int, total:int, status: str=''):
    '''
        Displays CLI progress bar with status. Count represents
        progress and total represents full scope.
    '''
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write(f'IBM Progress: [{bar}] {percents}%% ...{status}\r')
    sys.stdout.flush()

async def check_texts(texts: List[Tuple[str,str]], step: int=50)->List[Dict[str,str]]:
    '''
        Asyncronously sends texts to IBM entty and sentiment
        recognition service in batches of step-size. Returns
        results in List.
        texts = [(uuid, text),...]
    '''
    result = []
    index = 0
    while index < len(texts):
        progress(index, len(texts))
        tasks = []
        async with aiohttp.ClientSession(headers=HEADERS, auth=aiohttp.BasicAuth('apikey',API_KEY)) as session:
            for text in texts[index:index+step]:
                task = asyncio.create_task(check_text(text=text[1],uuid=text[0], session=session))
                tasks.append(task)
            try:
                batch_result = await asyncio.gather(*tasks)
                for doc_result in batch_result:
                    result.extend(doc_result)
                index += step
            # In case of network error or exceeding IBM limits
            # We wait for 10 seconds and retry current batch.
            except Exception as e:
                sleep(10)
                logging.exception(f'Got exception processing batch {texts[index:index+step]}')
            
    progress(index, len(texts))
    print('\n')
    return result