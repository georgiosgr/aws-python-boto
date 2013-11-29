'''
Created on Nov 29, 2013

@author: anuvrat
'''

import ConfigParser
import hashlib
from time import sleep

from boto.emr.connection import EmrConnection
from boto.emr.step import HiveStep, InstallHiveStep
from boto.s3.connection import S3Connection
from boto.s3.key import Key

def getMD5Checksum(filename):
    with open(filename) as openedFile:
        data = openedFile.read()
        checksum = hashlib.md5(data).hexdigest()
    return checksum

def getConfigParser(filename):
    config = ConfigParser.RawConfigParser()
    config.read(filename)
    return config

def getAWSCredentials(cfgFile = 'credentials.cfg'):
    config = getConfigParser(cfgFile) 
    return config.get('aws', 'access_key'), config.get('aws', 'secret_key')

def getHiveSetupStep():
    return InstallHiveStep(hive_versions='0.8.1.8')

def getQueryStep(connections, jobRunCfgFile = 'jobrun.cfg'):
    config = getConfigParser(jobRunCfgFile)
    
    hiveVersion = config.get('jobrun', 'hive_version')
    stepName = config.get('jobrun', 'step_name')
    scriptFile = config.get('jobrun', 'script_file')
    scriptArgs = config.get('jobrun', 'script_args').split(',')
    
    filePath = 's3://emr-script-run-files/' + uploadFileToS3(connections['s3'], scriptFile)
    
    return HiveStep(stepName, filePath, hive_versions = hiveVersion, hive_args = scriptArgs)

def connectToEMR(awsCfgFile = 'credentials.cfg'):
    accessKey, secretKey = getAWSCredentials(awsCfgFile)
    return EmrConnection(accessKey, secretKey)

def connectToS3(awsCfgFile = 'credentials.cfg'):
    accessKey, secretKey = getAWSCredentials(awsCfgFile)
    return S3Connection(accessKey, secretKey)

def uploadFileToS3(s3Connection, filename):
    bucket = s3Connection.get_bucket('emr-script-run-files')
    
    fName, ext = filename.split('.')
    keyValue = fName + '_' + getMD5Checksum(filename) + '.' + ext
    
    if bucket.get_key(keyValue): return keyValue
    
    k = Key(bucket)
    k.key = keyValue 
    
    k.set_contents_from_filename(filename)
    return keyValue

def createJobFlow(connections, steps = [], jobRunCfgFile = 'jobrun.cfg'):
    config = getConfigParser(jobRunCfgFile)
    
    logUri = 's3://emr-script-run-logs/'
    jobName = config.get('cluster', 'name')
    keyName = config.get('cluster', 'keyname')
    masterInstanceType = config.get('cluster', 'master_instance_type')
    slaveInstanceType = config.get('cluster', 'slave_instance_type')
    instancesCount = config.get('cluster', 'num_instances')
    actionFailure = config.get('cluster', 'action_on_failure')
    keepAlive = config.get('cluster', 'keep_alive')
    
    jobId = connections['emr'].run_jobflow(name = jobName, log_uri = logUri, ec2_keyname = keyName, 
                                           availability_zone = None, master_instance_type = masterInstanceType,
                                           slave_instance_type = slaveInstanceType, num_instances = instancesCount,
                                           action_on_failure = actionFailure, keep_alive = keepAlive,
                                           enable_debugging = False, hadoop_version = '1.0.3', steps = steps,
                                           bootstrap_actions = [], instance_groups = None, additional_info = None,
                                           ami_version = None, api_params = None, visible_to_all_users = None,
                                           job_flow_role = None)
    
    print 'Job logs will be available for the next few days at ' + logUri + '/' + jobId
    return jobId

def getStepLog(connections, keyValue):
    bucket = connections['s3'].get_bucket('emr-script-run-logs')

    if not bucket.get_key(keyValue): return
    
    k = Key(bucket)
    k.key = keyValue 
    
    content = k.get_contents_as_string()
    return content.strip()

def areSame(prevStatus, curStatus):
    if prevStatus is None: return False
    if curStatus.state != prevStatus.state: return False
    if len(curStatus.steps) != len(prevStatus.steps): return False
    for idx in xrange(len(curStatus.steps)):
        stepA = prevStatus.steps[idx]
        stepB = curStatus.steps[idx]
        if stepA.name != stepB.name or stepA.state != stepB.state: return False
    return True

def displayStepInfo(connections, jobId, currentStatus, step, stepIdx):
    print '\t' + '\t'.join(['Name: ', step.name, 'CreationTime: ', step.creationdatetime, 'State: ', step.state])
    if step.state in ['FAILED', 'TERMINATED', 'COMPLETED']:
        for logType in ['stdout', 'stderr']:
            logContent = getStepLog(connections, '/'.join([jobId, 'steps', str(stepIdx + 1), logType]))
            if logContent: print logType + ':\n' + logContent

def displayUsefulInfo(connections, jobId, currentStatus):
    print '\n' + '\t'.join(['JobFLowId: ', jobId, 'Name: ', currentStatus.name, 'State: ', currentStatus.state])
    print 'Steps: '
    for idx in xrange(len(currentStatus.steps)):
        displayStepInfo(connections, jobId, currentStatus, currentStatus.steps[idx], idx)
    print '\n'

def monitorJob(connections, jobId):
    previousStatus = None
    currentStatus = connections['emr'].describe_jobflow(jobId)
    while currentStatus.state not in ['FAILED', 'TERMINATED', 'COMPLETED']:
        if not areSame(previousStatus, currentStatus):
            displayUsefulInfo(connections, jobId, currentStatus)
        else:
            print '.',
        previousStatus = currentStatus
        sleep(10)
        currentStatus = connections['emr'].describe_jobflow(jobId)

if __name__ == '__main__':
    connections = {}
    connections['s3'] = connectToS3()
    connections['emr'] = connectToEMR()
    
    #steps = [getHiveSetupStep(), getQueryStep(connections)]
    #jobId = createJobFlow(connections, steps)
    jobId='j-3EHNOBV8FENWH'
    monitorJob(connections, jobId)
