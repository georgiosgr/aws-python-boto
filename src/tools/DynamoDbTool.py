'''
Created on Oct 4, 2013

@author: anuvrat
'''

import boto
from boto import dynamodb2

def getAllTableDescriptions(region):
    conn = boto.dynamodb2.connect_to_region( region.name )
    try:
        tables = conn.list_tables()
    except boto.exception.JSONResponseError:
        return {}
    
    descriptions = {}
    for tableName in tables['TableNames']:
        descriptions[tableName] = conn.describe_table( tableName )

    return descriptions

if __name__ == '__main__':
    for region in boto.dynamodb2.regions():
        descriptions = getAllTableDescriptions( region )
        if descriptions == {}:
            continue

        print region.name, descriptions
