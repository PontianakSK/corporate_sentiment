import asyncio
from asyncio.exceptions import TimeoutError
import logging
import requests
import sys
from time import sleep
from typing import Dict, List, Optional

import aiohttp
from aiohttp.client_exceptions import ClientPayloadError


WIKI_URL = 'http://www.wikidata.org/w/api.php'
SEARCH_LIMIT = '1' #Number of entities to request from wikidata
WIKIDATA_PROPERTIES = {
    'P946': 'ISIN', #International Securities Identification Number
    'P452': 'industry'
}
INSTANCE_OF_CODE = 'P31' #'instance of',that class of which this subject is a particular example and member
CLASS_CODES = {
    'Q4830453': 'business',
    'Q891723': 'public company',
    'Q6881511': 'enterprise',
    'Q161726': 'multinational corporation',
    
    
}

def parse_classes(instance_of: List[Dict])->Optional[str]:
    '''
        Checks if business, public company, enterprise
        or multinational corporation presents in instance_of 
        object. Returns string with classes that are present.
    '''
    parents = list()
    if not instance_of:
        return None
    for parent in instance_of:
        try:
            parent_code = parent['mainsnak']['datavalue']['value']['id']
        except KeyError:
            parent_code = None
        parent_name = CLASS_CODES.get(parent_code)
        if parent_name: parents.append(parent_name)
    return ', '.join(parents)

def parse_properties(entity: Dict)->Dict[str,str]:
    '''
        Checks all properties of entity.
        Returns dict:
        {
            'label': Name of entity
            'is_company': True if entity is organization and False otherwise 

        } 
    '''
    organization = dict()
    claims = entity.get('claims')
    try:
        organization['label'] = entity['labels']['en']['value']
    except KeyError:
        organization['label'] = None
    props = list()
    for prop_name, prop_description in WIKIDATA_PROPERTIES.items():
        if claims:
            prop_value = bool(claims.get(prop_name,None))
        else:
            prop_value = False
        props.append(prop_value)
    parents = claims.get(INSTANCE_OF_CODE) if claims else []
    props.append(bool(parse_classes(parents)))
    organization['is_company'] = any(props)
    return organization


def parse_entities(entities: Dict[str,Dict])->List[Dict]:
    '''
        Applies parse_properties to each of entities and returns
        list with results.
    '''
    entities_struct = list()
    for _,entity in entities.items():
        entity_struct = parse_properties(entity)
        entities_struct.append(entity_struct)
    return entities_struct

async def async_search(session: aiohttp.ClientSession, text: str)->Dict:
    logging.debug(f'Searching wiki for {text}')
    '''
        Sends search request with text to wikidata. Returns 
        wikidata response as a dict
    '''
    params = {
        'action': 'wbsearchentities',
        'language': 'en',
        'format': 'json',
        'search': text,
        'props': 'url',
        'limit': SEARCH_LIMIT,
    }
    try:
        async with session.get(WIKI_URL, allow_redirects=True,params=params) as response:
            data = await response.json()
            logging.debug(f'Search success: {text}!')
    except ValueError:
        logging.exception(f'Wrong text value: {text}')
        data = None
    return data

async def async_get_entity(session: aiohttp.ClientSession, ids: str)->Dict:
    '''
        Request wikidata entities under recieved ids. Returns dict
        with entities. 
    '''
    logging.debug(f'Getting entity data for ids:{ids}')
    params = {
        'action': 'wbgetentities',
        'sites': 'enwiki',
        'languages': 'en',
        'format': 'json',
        'ids': ids,
        #info|sitelinks|aliases|labels|descriptions|claims|datatype - possible props
        'props': 'labels|claims',
    }
    
    timeout = aiohttp.ClientTimeout(10)
    async with session.get(WIKI_URL, allow_redirects=True,params=params,timeout=timeout) as response:
        data = await response.json()
    logging.debug(f'Entities successfully got: {ids}!')
    return data

async def async_check_company(name: str, session: aiohttp.ClientSession)->Optional[List[Dict]]:
    '''
        Searches for entities with recieved name on wikidata.
        Checks if these entities are organizations and returns
        list with normalized entity names and results of check.
    '''
    logging.debug(f'Checking company {name} on wiki')
    search_response = await async_search(session,name)
    try:
        search_results = search_response['search']
    except (ValueError,TypeError):
        logging.exception(f'Can not retrieve data from search_results: {name}')
        return None
    if not search_results:
        return None
    entities_codes = '|'.join([entity['id'] for entity in search_results])
    entities_json = await async_get_entity(session,entities_codes)
    entities = entities_json['entities']
    company_data = parse_entities(entities)
    logging.debug(f'Company {name} successfully checked!')
    return company_data

def progress(count:int, total:int, status: str=''):
    '''
        Displays CLI progress bar with status. Count represents
        progress and total represents full scope.
    '''
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write(f'Wiki Progress: [{bar}] {percents}%% ...{status}\r')
    sys.stdout.flush()

async def async_check_companies(names: List[str], init_step: int=50)->List[Dict[str,str]]:
    '''
        Searches wikidata for entities with each of names. Checks if founded
        entity is organization and returns list of dicts with normalized
        names and results of check: {'label': , 'is_company':}.
        init_step stands for number of simultanious sessions.
    '''
    step = init_step
    result = []
    index = 0
    while index < len(names):
        progress(index, len(names))
        tasks = []
        async with aiohttp.ClientSession() as session:
            batch = names[index:index+step]
            for name in batch:
                task = asyncio.create_task(async_check_company(name, session))
                tasks.append(task)
            try:
                batch_result = await asyncio.gather(*tasks)
                result.extend(batch_result)
                index += step
            except (ClientPayloadError, TimeoutError):
                logging.exception('async_check_companies')
                if step > 1:
                    step = int(max(1,step/10))
                else:
                    result.append([{'label':'', 'is_company':False}])
                    index+=step
                    step = init_step
            except Exception as e:
                logging.exception('High level exception')
                sleep(10)

            
    progress(index, len(names))
    print('\n')
    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    companies = [
        'Yandex',
        'Google',
        'Ichkere',
        'Sberbank',
        'Shell',
        'Common Sense',
        'EU',
        'America',
        'Paramount Pictures',
        'Fedex',
        'Sendit',
    ]
    result = asyncio.run(async_check_companies(companies,11))
    print(result)

