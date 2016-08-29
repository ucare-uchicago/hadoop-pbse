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
           # match attempt
           ct.attemptId = str(container['attempt']).split('_')[3] + '_' + str(int(str(container['attempt']).split('_')[4])) + '_' + str(int(str(container['attempt']).split('_')[5]))
           ct.wholeAttemptId = container['attempt']
           if self.slowNode in container['mapnode']:
             ct.onSlowNode = True
             # done reading, now add to job list
           containers.append(ct)
         job.containers = job.containers + containers
         allJobs.append(job)
      # done
      # return the job
      return allJobs
   except Exception as e:
       print "Failed to json file [" + self.fileName + "] cause of: " + str(e)
       traceback.print_exc()
       return None