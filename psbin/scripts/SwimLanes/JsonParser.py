# json file parse class
import json
import datetime
import Job
import Container
import datetime
import traceback

class JsonParser:
  fileName = ''
  slowNode = ''
  def __init__(self, fileName, slowNode):
   self.fileName = fileName
   self.slowNode = slowNode

  def strToDate(self, datestr):
   return datetime.datetime.strptime(datestr, "%Y-%m-%d %H:%M:%S.%f")

  def parseJsonFile(self):
   try:
     with open(self.fileName) as data_file:    
      data = json.load(data_file)
      allJobs = []
       # create new job for each app
      for appName, app in data['apps'].items():
         # get the times from master
         job = Job.Job()
         job.jobDurationAM = (self.strToDate(app['master']['time_stop']) - self.strToDate(app['master']['time_start'])).total_seconds()
         job.jobDurationJC = int(app['master']['job_duration'])
         job.jobId = appName[-3:]
         job.jobStart = app['master']['time_start']
         job.containersKilledBySlowShuffle = app['master']['killedBySlowShuffle']
         job.slowShuffleDetectionTime = app['master']['slowShuffleDetections']
         job.launchedByWriteDiversity = app['master']['speculatedDueToWriteDiversity']
         job.writeDiversityDetectionTime = app['master']['writeDiversityDetections']
         # riza: added for query
         job.launchDuration = float(app["master"]["launch_duration"])
         job.commitDuration = float(app["master"]["commit_duration"])

         # get time for each container
         containers = []
         for ctName, container in app['containers'].items():
           # get only relevant data
           if ('time_stop' not in container) or ('time_start' not in container): 
             continue
           ct = Container.Container()
           ct.containerTime = (self.strToDate(container['time_stop']) - self.strToDate(container['time_start'])).total_seconds()
           ct.isMap = container['ismap']
           ct.isReduce = not container['ismap']
           ct.startDate = container['time_start']
           ct.onSlowNode = False
           ct.isSuccessful = container['isSuccessful']
           ct.shuffleFinishTime = container['shuffleEndTime']
           ct.sortFinishTime = container['sortEndTime']
           ct.reduceFinishTime = container['reduceEndTime']
           # match attempt
           if container['attempt'] is None or container['attempt'] == '':
             container['attempt'] = 'attempt_0_0_g_000000_0' 
             print "Failed to parse container on job " + str(job.jobId) + ": No attempt id!"
           ct.attemptId = str(container['attempt']).split('_')[3] + '_' + str(int(str(container['attempt']).split('_')[4])) + '_' + str(int(str(container['attempt']).split('_')[5]))
           ct.wholeAttemptId = container['attempt']

           # riza: where did this container run??
           ct.node = container['mapnode'] if ct.isMap else container['reducenode']
           if (self.slowNode in ct.node):
             ct.onSlowNode = True

           # added by Riza
           if (ct.isMap):
             ct.touchSlowNode = container['isSlowDatanode']
           else:
             ct.touchSlowNode = container['isSlowPipeline']
           if container['status_update'] != []:
             ct.firstHeartbeat = container['status_update'][0]
             ct.statusUpdate = container['status_update']

           containers.append(ct)
         # done reading, now add to job list
         job.containers = job.containers + containers
         allJobs.append(job)
      # done
      # return the job
      return allJobs
   except Exception as e:
       print "Failed to json file [" + self.fileName + "] cause of: " + str(e)
       traceback.print_exc()
       return None
