from core.core import get_logger
from core.config import SUBSCAN_API_KEY
import traceback
import requests
import json

# def getinfo(network="", endpoint="", json_data={}, version=2):
#     """Query data on chain in Polkadot's Ecosystem by Subscan.io API

#     Args:
#         network (str, optional): Choose network to query like polkadot, kusama. Defaults to "".
#         endpoint (str, optional): endpoint API provided by subscan's docs api. Defaults to "".
#         json_data (dict, optional): payload in json format. Defaults to {}.
#         version (int, optional): Subscan has 2 version of API - 1 and 2, choose one of them. Defaults to 2.

#     Raises:
#         ValueError: _First when api version is not 1 or 2_
#         ValueError: _Something wrong encountered wheen trying to request subscan.io_

#     Returns:
#         _type_: data fetch in json format
#     """
    
#     headers = {
#         "Content-Type": "application/json",
#         "X-API-Key": SUBSCAN_API_KEY
#     }
#     data = {}
#     try:
#         if version == 2:
#             response = requests.post(f'https://{network}.api.subscan.io/api/v2/scan{endpoint}', headers=headers,
#                                 json=json_data)
#         elif version == 1:
#             response = requests.post(f'https://{network}.api.subscan.io/api/scan{endpoint}', headers=headers,
#                                 json=json_data)
#         else:
#             raise ValueError("Version api hasn't been accepted!")
        
#         item = json.loads(response.text)

#         if item.get("message", "Error occurs at endpoint!") != "Success":
#             raise ValueError(f"{item.get('message', 'Error occurs at endpoint!')}")
#         else:
#             data = item.get("data")
#     except Exception as e:
#         get_logger().error(e)
#         traceback.print_exc()
        
#     return data

import random

def getinfo(network="", endpoint="", json_data={}, version=2):
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.5359.79 Mobile Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13.0; rv:107.0) Gecko/20100101 Firefox/107.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1462.46',
    ]
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    data = {}
    try:
        if version == 2:
            response = requests.post('https://' + network + '.webapi.subscan.io/api/v2/scan' + endpoint, headers=headers,
                                json=json_data)
        elif version == 1:
            response = requests.post('https://' + network + '.webapi.subscan.io/api/scan' + endpoint, headers=headers,
                                json=json_data)
        else:
            raise ValueError("Version api hasn't been accepted!")
        # return response
        # print( response.text, '1luot')
        item = json.loads(response.text)

        if item.get("message", "Error occurs at endpoint!") != "Success":
            raise ValueError(f"{item.get('message', 'Error occurs at endpoint!')}")
        else:
            data = item.get("data")
    except Exception as e:
        get_logger().error(e)
        traceback.print_exc()
    return data
