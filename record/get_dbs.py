#!/usr/bin/python
import pymongo
import string
import sys

from argparse import ArgumentParser

def get_args():
    parser = ArgumentParser(description='print a list of dbs on a host, '
                            'for use in config.py')

    parser.add_argument('-n', '--hostname', dest='hostname', default='localhost',
                      help='target host - default localhost', metavar='HOSTNAME')
    parser.add_argument('-p', '--port', dest='port', type=int, default=27017,
                      help='target port - default 27017', metavar='PORT')
    parser.add_argument('-x', '--exclude_list', dest='exclude_list',
                        help='comma separated list of databases to exclude - '
                        'default local,test', metavar='EXCLUDE_LIST',
                        default='local,test')

    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()
    try: 
        client = pymongo.MongoClient(args.hostname, args.port)
        
        db_names = client.database_names()
        exclude_list = string.split(args.exclude_list, ',')
        print(list(set(db_names) - set(exclude_list)))
        client.close()        
    except pymongo.errors.PyMongoError as e:
        print(e)
        sys.exit(1)
        
