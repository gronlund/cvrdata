from elasticsearch1 import Elasticsearch
from elasticsearch1_dsl import Search
import os
import ujson as json
import logging
import argparse
import tqdm

def download_all_dicts_to_file(filename, search, mode='w'):
    """ Download data from elastic search server

    :param filename: str, name of file to save data
    :param search: elasticsearch search object to query
    :return:
    """
    print('Download Data Write to File')
    print('ElasticSearch Download Scan Query: ', search.to_dict())
    generator = search.scan()
    #filename_tmp = '{0}_tmp.json'.format(filename)
    with open(filename, mode) as f:
        for obj in tqdm.tqdm(generator):
            json.dump(obj.to_dict(), f)
            f.write('\n')
    print('Updates Downloaded - File {0} written'.format(filename))
    return filename


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-user',  dest='cvruser', help='elastic user', type=str, default='dummy')
    parser.add_argument('-pass', dest='cvrpass', help='elastic password', type=str, default='dummy')
    args = parser.parse_args()
    # command line arguments run with python cvr_download.py -u <username> -p <password>
    logging.basicConfig(level=logging.INFO)

    url = 'http://distribution.virk.dk:80'
    index = 'cvr-permanent'
    # Make Elastic Client - arg names should be understandable - you can just hardcode them
    elastic_client = Elasticsearch(url, http_auth=(args.cvruser, args.cvrpass), timeout=60, max_retries=10, retry_on_timeout=True)
    # chunk size
    elastic_search_scan_size = 128
    # server keep alive time
    elastic_search_scroll_time = u'5m'
    # place to store file
    filename = os.path.join('./', 'cvr_all.json')
    # set elastic search params
    params = {'scroll': elastic_search_scroll_time, 'size': elastic_search_scan_size}
    # create elasticsearch search object
    search = Search(using=elastic_client, index=index)
    search = search.query('match_all')
    search = search.params(**params)
    download_all_dicts_to_file(filename=filename, search=search)
