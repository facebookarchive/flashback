#!/usr/bin/python
import pymongo
import string
import sys

from argparse import ArgumentParser

def get_args():
    parser = ArgumentParser(description='Run health checks of mongo and mtools systems')
    
    parser.add_argument('-a', '--action', dest='action', required=True,
                      help='action to take (enable|disable)',
                      metavar='(enable|disable)')
    parser.add_argument('-n', '--hostname', dest='hostname', default='localhost',
                      help='target host - default localhost', metavar='HOSTNAME')
    parser.add_argument('-p', '--port', dest='port', type=int, default=27017,
                      help='target port - default 27017', metavar='PORT')
    parser.add_argument('-s', '--size', dest='size', default=16777216, type=int,
                        metavar='SIZE_BYTES',
                        help='desired byte size of the system.profile collection'
                        ' - default 16777216 bytes')
    parser.add_argument('-x', '--exclude_list', dest='exclude_list',
                        help='comma separated list of databases to exclude - '
                        'default local,test', metavar='EXCLUDE_LIST',
                        default='local,test')
    
    args = parser.parse_args()
   
    if args.action not in ['enable', 'disable']:
        print("Unknown action %s" % args.action)
        sys.exit(1)

    return args

if __name__ == '__main__':
    args = get_args()
    
    try: 
        client = pymongo.MongoClient(args.hostname, args.port)
        
        db_names = client.database_names()
        exclude_list = string.split(args.exclude_list, ',')
        
        for db_name in db_names:
            if db_name not in exclude_list:
                db = client[db_name]
                
                if args.action == 'enable':
                    # can only drop/create the system.profile collection on the primary
                    if client.is_primary:
                        db.drop_collection('system.profile')
                        db.create_collection('system.profile', capped=True, size=args.size)
                    db.command('profile', 2)
                else:
                    db.command('profile', 0)
                    if client.is_primary:
                        db.drop_collection('system.profile')
        
        client.close()        
    except pymongo.errors.PyMongoError as e:
        print(e)
        sys.exit(1)
        
