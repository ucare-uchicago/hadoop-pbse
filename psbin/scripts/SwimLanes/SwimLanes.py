#!/usr//bin/python
import sys
import Job
import Container
import JsonParser
import datetime
import itertools
import math
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from pprint import pprint

def setFigureLabel(fig,suptitle,axtitle,xlabel,ylabel):
   fig.suptitle(suptitle, fontsize=14, fontweight='bold')
   ax = fig.add_subplot(111)
   fig.subplots_adjust(top=0.85)
   ax.set_xlabel(xlabel)
   ax.set_ylabel(ylabel)
   ax.set_title(axtitle)

if __name__ == "__main__":
    # figures
    figs = []
    # json file name
    jsonFileName = sys.argv[1]
    slowNode = sys.argv[2]
    print "args are : [" + jsonFileName + "], [" + slowNode + "]"  
    jsonParser = JsonParser.JsonParser(jsonFileName, slowNode)
    allJobs = jsonParser.parseJsonFile()
    print "Found " + str(len(allJobs)) + " jobs to analyze"
    # sort jobs by app id
    jobsSortedById = sorted(allJobs, key=lambda x: int(x.jobId))
    jobCount = 0
    # now, iterate jobs
    for job in jobsSortedById:
      # riza: filter jobs to display here
#      if job.launchDuration > 8 or job.commitDuration > 5 or job.jobDurationAM < 25 or job.jobDurationAM > 30 :
#      if job.jobDurationAM > 23:
#         continue

      # riza: sort by node first
      # containersSortedByStartDate = sorted(job.containers, \
      #     key=lambda x: (x.node, datetime.datetime.strptime(x.startDate, "%Y-%m-%d %H:%M:%S.%f")))

      # riza: sort by time first
      containersSortedByStartDate = sorted(job.containers, \
         key=lambda x: datetime.datetime.strptime(x.startDate, "%Y-%m-%d %H:%M:%S.%f"))

      print "Found " + str(len(containersSortedByStartDate)) + " containers for job " + job.jobId
      # first container will be up in the graph
      firstYCoordinate = len(containersSortedByStartDate)
      isZero = True
      # adjunst figure size, if not it could be caotic
      if len(containersSortedByStartDate) > 10:
        fig = plt.figure(figsize=(20, 16))
      else:
        fig = plt.figure(figsize=(12, 10))
      # ids killed by slow shuffle
      killedBySlowShuffle = []
      # ids launched by write diversity
      launchedByWriteDiversity = []
      # so, first container is the one that starts everything
      timeZero = ''
      for container in containersSortedByStartDate:
        allY = []
        allX = []
        if isZero == True:
          # first
          containerX = range(int(math.ceil((jsonParser.strToDate(container.startDate) - jsonParser.strToDate(job.jobStart)).total_seconds())), int(math.ceil((jsonParser.strToDate(container.startDate) - jsonParser.strToDate(job.jobStart)).total_seconds())) + int(math.ceil(container.containerTime)), 1)
          isZero = False
          timeZero = job.jobStart
        else:
          # others
          containerX = range(int(math.ceil((jsonParser.strToDate(container.startDate) - jsonParser.strToDate(timeZero)).total_seconds())), int(math.ceil(container.containerTime)) + int(math.ceil((jsonParser.strToDate(container.startDate) - jsonParser.strToDate(timeZero)).total_seconds())), 1)
        allY = allY + [firstYCoordinate] * len(containerX)
        allX = allX + containerX
        firstYCoordinate = firstYCoordinate - 1
        plt.xlim([0, job.jobDurationAM])
        plt.ylim([0, len(containersSortedByStartDate) + 1])
        # plot, set colors and line styles
        if len(allX) > 1 and len(allY) > 1:
          if container.isReduce == True and container.isSuccessful == True:
            plt.plot(allX, allY, color='red', linestyle='-', linewidth=2.0)
          elif container.isReduce == True and container.isSuccessful == False: 
            plt.plot(allX, allY, color='red', linestyle='--', linewidth=1.0)
          # check detection time in case this reduce was launched due to write diversity
          if container.isReduce == True and (container.wholeAttemptId in job.launchedByWriteDiversity):
            launchedByWriteDiversity.append(container.attemptId)
          # maps are special, since they can be killed by slow shuffle
          if container.isMap == True and container.isSuccessful == True and (container.wholeAttemptId in job.containersKilledBySlowShuffle):
            plt.plot(allX, allY, color='green', linestyle='-', linewidth=2.0)
            killedBySlowShuffle.append(container.attemptId)
          elif container.isMap == True and container.isSuccessful == True and (container.wholeAttemptId not in job.containersKilledBySlowShuffle):
            plt.plot(allX, allY, color='blue', linestyle='-', linewidth=2.0)
          elif container.isMap == True and container.isSuccessful == False: 
            plt.plot(allX, allY, color='blue', linestyle='--', linewidth=1.0)
          # remove y axis labels
          cur_axes = plt.gca()
          cur_axes.axes.get_yaxis().set_ticks([])
          # annotate
          if container.onSlowNode == True:
            plt.annotate(container.attemptId if not container.touchSlowNode else container.attemptId+"*", \
              xy=(allX[-1] + 1, allY[-1]), xytext=(allX[-1] + 1, allY[-1]), bbox=dict(facecolor='red', alpha=0.5))
          else:
            plt.annotate(container.attemptId if not container.touchSlowNode else container.attemptId+"*", \
              xy=(allX[-1] + 1, allY[-1]), xytext=(allX[-1] + 1, allY[-1]))
          # in here, we add markers for reducer phases
          if container.isReduce == True:
            reduceTime = container.reduceFinishTime
            shuffleTime = container.shuffleFinishTime
            sortTime = container.sortFinishTime
            reduceX = -1
            shuffleX = -1
            sortX = -1
            # get the points
            if shuffleTime is not None and shuffleTime != '':
              shuffleX = (jsonParser.strToDate(container.shuffleFinishTime) - jsonParser.strToDate(job.jobStart)).total_seconds()
              # print "::" + str(shuffleX)
              plt.plot(int(shuffleX), allY[-1], color='green', linestyle='dotted', linewidth=1.0, marker='o')
            if sortTime is not None and sortTime != '':
              sortX = (jsonParser.strToDate(container.sortFinishTime) - jsonParser.strToDate(job.jobStart)).total_seconds()
              # print "::" + str(sortX)
              plt.plot(int(sortX), allY[-1], color='yellow', linestyle='dotted', linewidth=1.0, marker='o')
            if reduceTime is not None and reduceTime != '':
              reduceX = (jsonParser.strToDate(container.reduceFinishTime) - jsonParser.strToDate(job.jobStart)).total_seconds()
              plt.plot(int(reduceX), allY[-1], color='blue', linestyle='dotted', linewidth=1.0, marker='o')
              # print "::" + str(reduceX)
          else:
            # riza: container is map, mark its first heartbeat
            for hbTime in container.statusUpdate:
              updateTime = (jsonParser.strToDate(hbTime) - jsonParser.strToDate(job.jobStart)).total_seconds()
              plt.plot(int(updateTime), allY[-1], color='cyan', linestyle='dotted', linewidth=1.0, marker='o')

          # in here, we add the write diversity detection point
          containerId = container.attemptId.split('_')[0] + "_" + container.attemptId.split('_')[1]
          attemptCount = int(container.attemptId.split('_')[2])
          for killedId in launchedByWriteDiversity:
            killedContainerId = killedId.split('_')[0] + "_" + killedId.split('_')[1]
            killedAttemptCount = int(killedId.split('_')[2])
            if containerId == killedContainerId and  attemptCount > killedAttemptCount:
              # get the index of the lane
              index  = 0
              for killed in job.launchedByWriteDiversity:
                kId = killed.split('_')[3] + "_" + str(int(killed.split('_')[4]))
                if containerId == kId:
                  break
                else:
                  index = index + 1
              # take zero time
              killedTime = job.writeDiversityDetectionTime[index]
              # so, we add a point at that y coordinate
              detectionTime = (jsonParser.strToDate(killedTime) - jsonParser.strToDate(job.jobStart)).total_seconds()
              plt.plot(int(detectionTime), allY[-1], color='m', linestyle='dotted', linewidth=1.0, marker='o')
              
          # in here, we add the slow shuffle detection point
          containerId = container.attemptId.split('_')[0] + "_" + container.attemptId.split('_')[1]
          attemptCount = int(container.attemptId.split('_')[2])
          for killedId in killedBySlowShuffle:
            killedContainerId = killedId.split('_')[0] + "_" + killedId.split('_')[1]
            killedAttemptCount = int(killedId.split('_')[2])
            if containerId == killedContainerId and  attemptCount > killedAttemptCount and container.isSuccessful == True:
              # get the index of the lane
              index  = 0
              for killed in job.containersKilledBySlowShuffle:
                kId = killed.split('_')[3] + "_" + str(int(killed.split('_')[4]))
                if containerId == kId:
                  break
                else:
                  index = index + 1
			        # take zero time
              killedTime = job.slowShuffleDetectionTime[index]
              # so, we add a point at that y coordinate
              detectionTime = (jsonParser.strToDate(killedTime) - jsonParser.strToDate(job.jobStart)).total_seconds()
              plt.plot(int(detectionTime), allY[-1], color='green', linestyle='dotted', linewidth=1.0, marker='o')
              # print killedContainerId + "--" + str(killedAttemptCount) + " : " + containerId + "--" + str(attemptCount) + "? " + str(index)
          
      # create figure
      setFigureLabel(fig,"Swimlane","job = " + str(job.jobId) + "(" + str(len(containersSortedByStartDate)) + " containers, duration =" + str(job.jobDurationAM) + " secs)",\
                     "Time (seconds)","")
      # append figure
      figs.append(fig)
      # done
      plt.close() 
      jobCount = jobCount + 1
      # if jobCount == 4:
      #  break
    # done, now print swimlanes  
    with PdfPages('swimLanes.pdf') as pdf:
      ct = 0
      for fig in figs:
        print "saving page %d" % ct
        pdf.savefig(fig)
        ct += 1 
