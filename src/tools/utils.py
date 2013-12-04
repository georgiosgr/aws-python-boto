'''
Created on Dec 4, 2013

@author: anuvrat
'''

import ConfigParser
import hashlib

from boto.emr.connection import EmrConnection
from boto.s3.connection import S3Connection
from boto.s3.key import Key

def connectToEMR(awsCfgFile = 'credentials.cfg'):
    accessKey, secretKey = getAWSCredentials(awsCfgFile)
    return EmrConnection(accessKey, secretKey)

def connectToS3(awsCfgFile = 'credentials.cfg'):
    accessKey, secretKey = getAWSCredentials(awsCfgFile)
    return S3Connection(accessKey, secretKey)

def getAWSCredentials(cfgFile = 'credentials.cfg'):
    config = getConfigParser(cfgFile) 
    return config.get('aws', 'access_key'), config.get('aws', 'secret_key')

def getConfigParser(filename):
    config = ConfigParser.RawConfigParser()
    config.read(filename)
    return config

def getMD5Checksum(filename):
    with open(filename) as openedFile:
        data = openedFile.read()
        checksum = hashlib.md5(data).hexdigest()
    return checksum

def uploadFileToS3(s3Connection, filename):
    bucket = s3Connection.get_bucket('emr-script-run-files')
    
    fName, ext = filename.split('.')
    keyValue = fName + '_' + getMD5Checksum(filename) + '.' + ext
    
    if bucket.get_key(keyValue): return keyValue
    
    k = Key(bucket)
    k.key = keyValue 
    
    k.set_contents_from_filename(filename)
    return keyValue