'''
Created on Nov 29, 2013

@author: anuvrat
'''

from time import sleep

from boto.emr.step import HiveStep, InstallHiveStep
from tools.utils import getConfigParser, uploadFileToS3, connectToS3, connectToEMR

from boto.s3.key import Key

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

def areSame(connections, prevStatus, curStatus, pendingLogs, logsDisplayed):
    if prevStatus is None: return False
    if curStatus.state != prevStatus.state: return False
    if len(curStatus.steps) != len(prevStatus.steps): return False
    for idx in xrange(len(curStatus.steps)):
        stepA = prevStatus.steps[idx]
        stepB = curStatus.steps[idx]
        if stepA.name != stepB.name or stepA.state != stepB.state: return False
    for pending in [l for l in pendingLogs if l not in logsDisplayed]:
        if getStepLog(connections, pending): return False
    
    return True

def displayStepInfo(connections, jobId, currentStatus, step, stepIdx, logsDisplayed, pendingLogs):
    print '\t' + '\t'.join(['Name: ', step.name, 'CreationTime: ', step.creationdatetime, 'State: ', step.state])
    if step.state in ['FAILED', 'TERMINATED', 'COMPLETED']:
        for logType in ['stdout', 'stderr']:
            keyValue = '/'.join([jobId, 'steps', str(stepIdx + 1), logType])
            if keyValue in logsDisplayed: continue
            logContent = getStepLog(connections, keyValue)
            if logContent: 
                print logType + ':\n' + logContent
                logsDisplayed.append(keyValue)
            else:
                pendingLogs.append(keyValue)

def displayUsefulInfo(connections, jobId, currentStatus, logsDisplayed, pendingLogs):
    print '\n' + '\t'.join(['JobFLowId: ', jobId, 'Name: ', currentStatus.name, 'State: ', currentStatus.state])
    print 'Steps: '
    for idx in xrange(len(currentStatus.steps)):
        displayStepInfo(connections, jobId, currentStatus, currentStatus.steps[idx], idx, logsDisplayed, pendingLogs)
    print '\n'

def monitorJob(connections, jobId):
    previousStatus = None
    currentStatus = connections['emr'].describe_jobflow(jobId)
    logsDisplayed = []
    pendingLogs = []
    progressDots = 0
    while currentStatus.state not in ['FAILED', 'TERMINATED', 'COMPLETED']:
        if not areSame(connections, previousStatus, currentStatus, pendingLogs, logsDisplayed):
            displayUsefulInfo(connections, jobId, currentStatus, logsDisplayed, pendingLogs)
        else:
            print '.',
            progressDots += 1
            if progressDots == 80:
                print '\n',
                progressDots = 0
        previousStatus = currentStatus
        sleep(5)
        currentStatus = connections['emr'].describe_jobflow(jobId)

if __name__ == '__main__':
    connections = {}
    connections['s3'] = connectToS3()
    connections['emr'] = connectToEMR()
    
    steps = [getHiveSetupStep(), getQueryStep(connections)]
    jobId = createJobFlow(connections, steps)
    monitorJob(connections, jobId)
