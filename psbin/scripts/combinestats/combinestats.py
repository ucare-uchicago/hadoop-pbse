#!/usr/bin/python

import os, re, sys
import pprint
import datetime
import json
from collections import defaultdict

import numpy as np
import matplotlib
matplotlib.use('Agg')
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

SLOWNODE=100
SLOWHOST="VOID"
SLOWIP="10.1.1."+str(SLOWNODE+2)

VERSION="4.0"

pp = pprint.PrettyPrinter(indent=2)

cassign = re.compile('.+ Assigned container (.+) to (.+)')
tasknode = re.compile('.+ TaskAttempt: \[(.+)\].+ on NM: \[(.+):.+\]')
dataread = re.compile('.+ reporting datanode .+\[(.+):50010.+\]')
re_date = re.compile("..+[-/]..[-/].. ..:..:..(,...)*")
re_hb = re.compile(".*statusUpdate.*")
re_am_finalct = re.compile(".+Final Stats: PendingReds:(.+) ScheduledMaps:(.+) ScheduledReds:(.+) AssignedMaps:(.+) AssignedReds:(.+) CompletedMaps:(.+) CompletedReds:(.+) ContAlloc:(.+) ContRel:(.+) HostLocal:(.+) RackLocal:(.+)")
re_am_specadd = re.compile(".+addSpeculativeAttempt.+")
re_jc_appid = re.compile(".+Submitted application (.+)")
re_jc_startrun = re.compile(".+Job .+ running in uber mode.+")
re_jc_stoprun = re.compile(".+Job .+ completed successfully")
re_jc_duration = re.compile("The job took (.+) seconds.")


NORMALIZE = False
CDF_Y_LIMIT = 1.0

def getTaskId(ct):
  att = ct["attempt"]
  return att[8:35]


def getLogTime(line):
  datestr = re_date.search(line).group(0)
  thedate = datetime.datetime.now()
  if "-" in datestr:
    thedate = datetime.datetime.strptime(datestr, "%Y-%m-%d %H:%M:%S,%f")
  else:
    thedate = datetime.datetime.strptime(datestr, "%y/%m/%d %H:%M:%S")

  return thedate.strftime("%Y-%m-%d %H:%M:%S.%f")


def strToDate(datestr):
  return datetime.datetime.strptime(datestr, "%Y-%m-%d %H:%M:%S.%f")


def getPlacementCode(ct):
  T = "S" if ct["mapnode"].startswith(SLOWHOST) else "F"
  d = ""
  if not ct["datanode"]:
    d = "n"
  elif SLOWIP == ct["datanode"][-1]:
    d = "s"
  else:
    d = "f"
  return T+d


def getTaskCode(lst):
  lst = sorted(lst,key=lambda x:x["attempt"])
  OTod = getPlacementCode(lst[0])
  BTbd = getPlacementCode(lst[1]) if len(lst)>1 else "nn"
  return OTod+"_"+BTbd


def printTask(ct):
  print ct["appid"]
  print ct["containerid"]
  print ct["attempt"]
  print ct["mapnode"]
  pp.pprint(ct["datanode"])
  print ""


def printTasks(apps):
  for appname,app in apps.items():
    for ctname,container in app["containers"].items():
      printTask(container)


def makeCDFPoints(vals):
  X = np.asarray(sorted(vals))
  Y = np.ones(len(vals))
  Y /= Y.sum()
  Y = np.cumsum(Y)
  X = np.insert(X,0,0)
  Y = np.insert(Y,0,0)
  if CDF_Y_LIMIT < 1.0:
    TX = []
    TY = []
    for i in xrange(0,len(X)):
      if (Y[i] <= CDF_Y_LIMIT):
        TX.append(X[i])
        TY.append(Y[i])
    X = np.array(TX)
    Y = np.array(TY)
  return (X,Y)


def setFigureLabel(fig,suptitle,axtitle,xlabel,ylabel):
  fig.suptitle(suptitle, fontsize=14, fontweight='bold')
  ax = fig.add_subplot(111)
  fig.subplots_adjust(top=0.85)
  if (NORMALIZE):
    xlabel += " (normalized)"
    print xlabel
  ax.set_xlabel(xlabel)
  ax.set_ylabel(ylabel)

  handles, labels = ax.get_legend_handles_labels()
  ax.legend(handles, labels, loc=4)

  if axtitle:
    ax.set_title(axtitle)


def combineGraphs(runs,selector,iscdf, xlim = -1):
  fig = plt.figure(figsize=(8, 6))
  maxval = -1
  color = []
  color.append('red')
  color.append('blue')
  color.append('green')
  color.append('magenta')
  color.append('black')
  color.append('orange')
  cont = 0
  for runid,data in sorted(runs.items(), key=lambda x:x[0]):
    dat = selector(data["apps"])
    X,Y = ([],[])
    X,Y = makeCDFPoints(dat)
    if (NORMALIZE):
      X = X.astype(float) / X.max()
#    plt.plot(X, Y, data["conf"]["style_line"], label=runid, color=color[cont])
    plt.plot(X, Y, data["conf"]["style_line"], label=runid, linewidth=float(data["conf"]["linewidth"]))
    cont = cont + 1
    if maxval < X.max():
      maxval = X.max()

  plt.xlim([X.min(),xlim if xlim > -1 else maxval*1.1])
  if iscdf:
    plt.ylim([Y.min(),1.0])

  return fig


def printCombinedGraphs(runs):
  figs = []

  TASKS = lambda apps: [task for aname,a in apps.items() \
             for tname,task in a["containers"].items()]
  MAPS = lambda apps: [a for a in TASKS(apps) if (a["ismap"])]
  REDUCES = lambda apps: [a for a in TASKS(apps) if (not a["ismap"])]
  AM = lambda apps: [a["master"] for aname,a in apps.items()]
  JC = lambda apps: [a["jobclient"] for aname,a in apps.items() if ("jobclient" in a)]
  ALL_JOBS = lambda apps: [a for aname,a in apps.items()]

  # Heartbeat CDF
  dat = lambda apps: \
        [len(a["status_update"]) for a in MAPS(apps) \
         if ("status_update" in a)]
  fig = combineGraphs(runs,dat,True,100)
  setFigureLabel(fig,"Heartbeat CDF","","#hb/task","percentage")
  figs.append(fig)
  plt.close()

  # Map Running Time
  dat = lambda apps: \
        [(strToDate(a["time_stop"])-strToDate(a["time_start"])) .total_seconds() \
         for a in MAPS(apps) if ("time_start" in a) and ("time_stop" in a)]
  fig = combineGraphs(runs,dat,True,100)
  setFigureLabel(fig,"Map Running Time","","second","percentage")
  figs.append(fig)
  plt.close()

  # Reduce Running Time
  dat = lambda apps: \
        [(strToDate(a["time_stop"])-strToDate(a["time_start"])) .total_seconds() \
         for a in REDUCES(apps) if ("time_start" in a) and ("time_stop" in a)]
  fig = combineGraphs(runs,dat,True,200)
  setFigureLabel(fig,"Reduce Running Time","","second","percentage")
  figs.append(fig)
  plt.close()

  # Shuffle Running Time
  dat = lambda apps: \
        [a["shuffleTime"] for a in REDUCES(apps) \
         if ("shuffleTime" in a) and ("time_start" in a) and ("time_stop" in a)]
  fig = combineGraphs(runs,dat,True,100)
  setFigureLabel(fig,"Shuffle Running Time","","second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time
  dat = lambda apps: \
        [(strToDate(a["time_stop"])-strToDate(a["time_start"])) .total_seconds() \
         for a in AM(apps) if ("time_start" in a) and ("time_stop" in a)]
  fig = combineGraphs(runs,dat,True,1000)
  setFigureLabel(fig,"Job Running Time (by AM)","","second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time excluding commit duration
  dat = lambda apps: \
        [(strToDate(a["time_stop"])-strToDate(a["time_start"])) .total_seconds() - a["commit_duration"]\
         for a in AM(apps) if ("time_start" in a) and ("time_stop" in a)]
  fig = combineGraphs(runs,dat,True,100)
  setFigureLabel(fig,"Job Running Time (Commit time not included)","","second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time excluding launch duration
  dat = lambda apps: \
        [(strToDate(a["time_stop"])-strToDate(a["time_start"])) .total_seconds() - a["launch_duration"]\
         for a in AM(apps) if ("time_start" in a) and ("time_stop" in a)]
  fig = combineGraphs(runs,dat,True,100)
  setFigureLabel(fig,"Job Running Time (Launch time not included)","","second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time excluding bot duration
  dat = lambda apps: \
        [(strToDate(a["time_stop"])-strToDate(a["time_start"])) .total_seconds() - a["launch_duration"] - a["commit_duration"]\
         for a in AM(apps) if ("time_start" in a) and ("time_stop" in a)]
  fig = combineGraphs(runs,dat,True,100)
  setFigureLabel(fig,"Job Running Time (Neither launch nor commit time included)","","second","percentage")
  figs.append(fig)
  plt.close()

  # Job commit duration
  dat = lambda apps: \
         [a["commit_duration"] \
          for a in AM(apps) if ("time_start" in a) and ("time_stop" in a)]

  fig = combineGraphs(runs,dat,True,30)
  setFigureLabel(fig,"Job commit duration","",\
       "second","percentage")
  figs.append(fig)
  plt.close()

  # Job launch duration
  dat = lambda apps: \
         [a["launch_duration"] \
          for a in AM(apps) if ("time_start" in a) and ("time_stop" in a)]

  fig = combineGraphs(runs,dat,True,20)
  setFigureLabel(fig,"Job launch duration","",\
       "second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time for jobs where the slow node is involved in map
  dat = lambda apps: \
         [(strToDate(a["master"]["time_stop"])-strToDate(a["master"]["time_start"])).total_seconds() \
          for a in ALL_JOBS(apps) if ("time_start" in a["master"]) and ("time_stop" in a["master"]) and (a["master"]["slowNodeInvolvedInMap"]) and (not a["master"]["slowNodeInvolvedInReduce"])]

  fig = combineGraphs(runs,dat,True,-1)
  setFigureLabel(fig,"Job Running Time (by AM, slow node involved as map only)","",\
			 "second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time for jobs where the slow node is involved in reduce
  dat = lambda apps: \
         [(strToDate(a["master"]["time_stop"])-strToDate(a["master"]["time_start"])).total_seconds() \
          for a in ALL_JOBS(apps) if ("time_start" in a["master"]) and ("time_stop" in a["master"]) and (not a["master"]["slowNodeInvolvedInMap"]) and (a["master"]["slowNodeInvolvedInReduce"])]

  fig = combineGraphs(runs,dat,True,-1)
  setFigureLabel(fig,"Job Running Time (by AM, slow node involved as reduce only)","",\
			 "second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time for jobs where the slow node is involved in reduce and map
  dat = lambda apps: \
         [(strToDate(a["master"]["time_stop"])-strToDate(a["master"]["time_start"])).total_seconds() \
          for a in ALL_JOBS(apps) if ("time_start" in a["master"]) and ("time_stop" in a["master"]) and (a["master"]["slowNodeInvolvedInMap"]) and (a["master"]["slowNodeInvolvedInReduce"])]

  fig = combineGraphs(runs,dat,True,-1)
  setFigureLabel(fig,"Job Running Time (by AM, slow node involved as reduce and map)","",\
			 "second","percentage")
  figs.append(fig)
  plt.close()

  # JobClient Duration
  dat = lambda apps: \
        [a["job_duration"] for a in JC(apps) if ("job_duration" in a)]
  fig = plt.figure(figsize=(8, 6))
  fig = combineGraphs(runs,dat,True,1000)
  setFigureLabel(fig,"Job Duration (by JobClient)","","second","percentage")
  figs.append(fig)
  plt.close()

  # Job Waiting Time
  dat = lambda apps: \
        [(strToDate(a["time_start"])-strToDate(a["time_submit"])).total_seconds() \
         for a in JC(apps) if ("time_start" in a) and ("time_submit" in a)]
  fig = plt.figure(figsize=(8, 6))
  fig = combineGraphs(runs,dat,True,-1)
  setFigureLabel(fig,"Job Waiting Time","","second","percentage")
  figs.append(fig)
  plt.close()

  # Locality percentage
  dat = lambda apps: \
        [float(a["ct_HostLocal"])/float(a["ct_CompletedMaps"]) \
         for a in AM(apps) if ("ct_HostLocal" in a) and ("ct_CompletedMaps" in a)]
  fig = plt.figure(figsize=(8, 6))
  fig = combineGraphs(runs,dat,True)
  setFigureLabel(fig,"Locality Percentage","","% locality","percentage")
  figs.append(fig)
  plt.close()

  # Map Spec Count
  dat = lambda apps: \
        [int(a["ct_SpecMap"]) for a in AM(apps)]
  fig = plt.figure(figsize=(8, 6))
  fig = combineGraphs(runs,dat,True)
  setFigureLabel(fig,"Map Speculation per Job","","#spec map","percentage")
  figs.append(fig)
  plt.close()


  with PdfPages('all_graphs.pdf') as pdf:
    ct = 0
    for fig in figs:
      print "saving page %d" % ct
      pdf.savefig(fig)
      #fig.savefig("fig-%d.png" % ct)
      ct += 1


def printTopoStats(apps):
  tasks = []
  for appname,app in apps.items():
    for ctname,container in app["containers"].items():
      tasks.append(container)

  d = {}
  for task in tasks:
    tid = getTaskId(task)
    if not task["ismap"]:
      continue
    if tid in d:
      d[tid].append(task)
    else:
      d[tid] = [task]

  ctr = defaultdict(int)
  for k,v in d.items():
    tcode = getTaskCode(v)
    ctr[tcode] += 1

    if tcode.startswith("Ss_Ss"):
    #if "_0034_m_000001" in k:
     for t in sorted(v,key=lambda x:x["attempt"]):
        printTask(t)

  total = 0
  for k,v in sorted(ctr.items(),key=lambda x:x[0]):
    print "%s\t: %d" % (k,v)
    total += v
  print "total\t: %d" % total

  print ""
  for k,v in sorted(ctr.items(),key=lambda x:x[0]):
    print "%s\t: %.1f" % (k,v/float(total)*100)


def loadExpStats():
  runs = {}
  for root, dirnames, filenames in os.walk('.'):
    for fname in filenames:
      if fname.endswith(".json"):
        with open(fname) as data_file:
          json_data = json.load(data_file)
          fileversion = json_data["conf"]["version"]
          if VERSION != fileversion:
            print "WARNING: %s has different version (%s) than combinestats version (%s), things might broken" \
              % (fname, fileversion, VERSION)
          runs[fname.replace(".json","")] = json_data
    break
  return runs

def main():
  runs = loadExpStats()
  printCombinedGraphs(runs)


def printContainers():
  tasks = getTopology()
  printTasks(tasks)

if __name__ == '__main__':
  main()
#  printContainers()
