#!/bin/env python
import sys
import Job
import Container
import JsonParser
import datetime
import itertools
import math
import numpy as np
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
      containersSortedByStartDate = sorted(job.containers, key=lambda x: datetime.datetime.strptime(x.startDate, "%Y-%m-%d %H:%M:%S.%f"))
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
      # so, first container is the one that starts everything
      timeZero = ''
      for container in containersSortedByStartDate:
        allY = []
        allX = []
        if isZero == True:
          # first
          containerX = range(int((jsonParser.strToDate(container.startDate) - jsonParser.strToDate(job.jobStart)).total_seconds()), int((jsonParser.strToDate(container.startDate) - jsonParser.strToDate(job.jobStart)).total_seconds()) + int(container.containerTime), 1)
          isZero = False
          timeZero = job.jobStart
        else:
          # others
          containerX = range(int((jsonParser.strToDate(container.startDate) - jsonParser.strToDate(timeZero)).total_seconds()), int(container.containerTime) + int((jsonParser.strToDate(container.startDate) - jsonParser.strToDate(timeZero)).total_seconds()), 1)
        allY = allY + [firstYCoordinate] * len(containerX)
        allX = allX + containerX
        firstYCoordinate = firstYCoordinate - 1
        plt.xlim([0, job.jobDurationAM + 1])
        plt.ylim([0, len(containersSortedByStartDate) + 1])
        # plot, set colors and line styles
        if len(allX) > 1 and len(allY) > 1:
          if container.isReduce == True and container.isSuccessful == True:
            plt.plot(allX, allY, color='red', linestyle='-', linewidth=2.0)
          elif container.isReduce == True and container.isSuccessful == False: 
            plt.plot(allX, allY, color='red', linestyle='--', linewidth=1.0)
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
            plt.annotate(container.attemptId, xy=(allX[-1] + 1, allY[-1]), xytext=(allX[-1] + 1, allY[-1]), bbox=dict(facecolor='red', alpha=0.5))
          else:
            plt.annotate(container.attemptId, xy=(allX[-1] + 1, allY[-1]), xytext=(allX[-1] + 1, allY[-1]))
          # in here, we add the slow shuffle detection point
          containerId = container.attemptId.split('_')[0] + "_" + container.attemptId.split('_')[1]
          attemptCount = int(container.attemptId.split('_')[2])
          for killedId in killedBySlowShuffle:
            killedContainerId = killedId.split('_')[0] + "_" + killedId.split('_')[1]
            killedAttemptCount = int(killedId.split('_')[2])
            if containerId == killedContainerId and  attemptCount > killedAttemptCount and container.isSuccessful == True:
              # take zero time
              index = killedBySlowShuffle.index(killedId)
              killedTime = job.slowShuffleDetectionTime[index]
              # so, we add a point at that y coordinate
              detectionTime = (jsonParser.strToDate(killedTime) - jsonParser.strToDate(job.jobStart)).total_seconds()
              plt.plot(int(detectionTime), allY[-1], color='green', linestyle='dotted', linewidth=1.0, marker='o')
      # create figure
      setFigureLabel(fig,"Swimlane","job = " + str(job.jobId) + "(" + str(len(containersSortedByStartDate)) + " containers, duration =" + str(job.jobDurationAM) + " secs)",\
                     "Time (seconds)","")
      # append figure
      figs.append(fig)
      # done
      plt.close() 
      jobCount = jobCount + 1
    # done, now print swimlanes  
    with PdfPages('swimLanes.pdf') as pdf:
      ct = 0
      for fig in figs:
        print "saving page %d" % ct
        pdf.savefig(fig)
        ct += 1 
