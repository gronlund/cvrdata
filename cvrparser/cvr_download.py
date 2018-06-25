from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import os
import ujson as json
import logging
import argparse
import tqdm


def download_all_dicts_to_file(filename, search, mode='w'):
    """ Download data from elastic search server

    :param filename: str, name of file to save data
    :param search: elasticsearch search object to query
    :param mode, char, file write mode (w, a)
    :return filename, str:
    """
    print('Download Data Write to File')
    print('ElasticSearch Download Scan Query: ', str(search.to_dict())[0:1000], ' ...')
    generator = search.scan()
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
    elastic_client = Elasticsearch(url, http_auth=(args.cvruser, args.cvrpass),
                                   timeout=60, max_retries=10, retry_on_timeout=True)
    # chunk size
    elastic_search_scan_size = 128
    # server keep alive time
    elastic_search_scroll_time = u'5m'
    # place to store file
    json_filename = os.path.join('./', 'cvr_all.json')
    # set elastic search params
    params = {'scroll': elastic_search_scroll_time, 'size': elastic_search_scan_size}
    # create elasticsearch search object
    el_search = Search(using=elastic_client, index=index)
    el_search = el_search.query('match_all')
    el_search = el_search.params(**params)
    download_all_dicts_to_file(filename=json_filename, search=el_search)
