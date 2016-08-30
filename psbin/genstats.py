#!/usr/bin/python

import os, re, sys
import pprint
import datetime
import json
from collections import defaultdict
from collections import Counter

import numpy as np
import matplotlib
matplotlib.use('Agg')
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

SLOWNODE=100
SLOWHOST="VOID"
SLOWIP="10.1.1."+str(SLOWNODE+2)

VERSION="2.3"

pp = pprint.PrettyPrinter(indent=2)

cassign = re.compile('.+ Assigned container (.+) to (.+)')
tasknode = re.compile('.+ TaskAttempt: \[(.+)\].+ on NM: \[(.+):.+\]')
dataread = re.compile('.+ reporting datanode (.+)')
re_date = re.compile("..+[-/]..[-/].. ..:..:..(,...)*")
re_hb = re.compile(".*statusUpdate.*")
re_tags_pbse = re.compile(".+ (PBSE-[^ :]+).*")
re_am_finalct = re.compile(".+Final Stats: PendingReds:(.+) ScheduledMaps:(.+) ScheduledReds:(.+) AssignedMaps:(.+) AssignedReds:(.+) CompletedMaps:(.+) CompletedReds:(.+) ContAlloc:(.+) ContRel:(.+) HostLocal:(.+) RackLocal:(.+)")
re_am_specadd = re.compile(".+addSpeculativeAttempt.+")
re_jc_appid = re.compile(".+Submitted application (.+)")
re_jc_startrun = re.compile(".+Job .+ running in uber mode.+")
re_jc_stoprun = re.compile(".+Job .+ completed successfully")
re_jc_duration = re.compile("The job took (.+) seconds.")
re_shffle_duration = re.compile(".+ PBSE_SHUFFLE:\{(.+)\:(.+),(.+)\:(.+)\}")
lookForShuffleTime = "\"SHUFFLE_TIME\""
re_container_finished = re.compile(".+Task \'(.+)\' done\.")
re_app_master_node = re.compile(".+Instantiated MRClientService at (.+)/(.+)")
re_relauch_attempt = re.compile(".+Relaunching attempt (.+) of task (.+) at host (.+)")

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
  elif ct["datanode"][-1].startswith(SLOWHOST):
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

def getMasterStats(app):
  appname = app["appid"]
  master = app["master"]
  syslog = os.path.join("yarn/userlogs", \
                          appname,master["syslogdir"],"syslog")
  if not os.path.exists(syslog):
    return
  f = open(syslog)

  # init dict
  master["appid"] = appname
  master["ct_SpecMap"] = 0
  master["ct_SpecRed"] = 0
  master["isInvolveSlownode"] = False
  master["slowNodeInvolvedInDataread"] = False
  master["slowNodeInvolvedInMap"] = False
  master["slowNodeInvolvedInReduce"] = False
  master["tags_PBSE"] = []
  # location
  master["location"] = ""
  # killed by slow shuffle
  master["killedBySlowShuffle"] = []
  # time where the kill was issued
  master["slowShuffleDetections"] = []

  linect = 0
  for line in f:
    if linect == 0:
      master["time_start"] = getLogTime(line)
    # get the master node
    match = re_app_master_node.match(line)
    if match:
      master["location"] = match.group(1); 
    match = re_relauch_attempt.match(line)
    if match:
      master["killedBySlowShuffle"].append(match.group(1))
      master["slowShuffleDetections"].append(getLogTime(line))
    match = cassign.match(line)
    if match:
      ct = match.group(1)
      att = match.group(2)
      #print ct, att
      if ct in app["containers"]:
        container = app["containers"][ct]
        container["attempt"] = att
        container["ismap"] = ("_m_" in att)
      else:
        # killed container
        print "killed: "+appname+"/"+ct
        container = {
          "isKilled": True,
          "appid": app["appid"],
          "containerid": ct,
          "attempt": att,
          "mapnode": "",
          "reducenode": "",
          "datanode": [],
          "lastDatanode": "NONE",
          "ismap" : ("_m_" in att),
          "shuffleTime": 0.0,
          "isSuccessful": False,
          "isSlowMapnode": False,
          "isSlowDatanode": False,
          "isSlowReducenode": False
        }
        app["containers"][ct] = container

    match = tasknode.match(line)
    if match:
      att = match.group(1)
      workernode = match.group(2)
      for cname,ctr in app["containers"].items():
        if ctr["attempt"] == att:
          if ctr["ismap"]:
            ctr["mapnode"] = workernode
            if workernode.startswith(SLOWHOST):
              ctr["isSlowMapnode"] = True
              master["slowNodeInvolvedInMap"] = True
          else:
            ctr["reducenode"] = workernode
            if workernode.startswith(SLOWHOST):
              ctr["isSlowReducenode"] = True
              master["slowNodeInvolvedInReduce"] = True

    match = re_am_finalct.match(line)
    if match:
      master["ct_PendingReds"] = match.group(1)
      master["ct_ScheduledMaps"] = match.group(2)
      master["ct_ScheduledReds"] = match.group(3)
      master["ct_AssignedMaps"] = match.group(4)
      master["ct_AssignedReds"] = match.group(5)
      master["ct_CompletedMaps"] = match.group(6)
      master["ct_CompletedReds"] = match.group(7)
      master["ct_ContAlloc"] = match.group(8)
      master["ct_ContRel"] = match.group(9)
      master["ct_HostLocal"] = match.group(10)
      master["ct_RackLocal"] = match.group(11)

    match = re_am_specadd.match(line)
    if match:
      if "_m_" in match.group(0):
        master["ct_SpecMap"] += 1
      else:
        master["ct_SpecRed"] += 1

    match = re_tags_pbse.match(line)
    if match:
      if match.group(1) not in master["tags_PBSE"]:
        master["tags_PBSE"].append(match.group(1))

    linect += 1

  master["time_stop"] = getLogTime(line)
  master["job_duration"] = (strToDate(master["time_stop"])-strToDate(master["time_start"])).total_seconds()


def getContainerStats(app):
  appname = app["appid"]
  master = app["master"]
  # set flags to know if slow node is involded

  for ctname,ct in app["containers"].items():
    syslog = os.path.join("yarn/userlogs", \
                            appname,ctname,"syslog")
    if not os.path.exists(syslog):
      continue
    f = open(syslog)

    linect = 0
    line = ""
    ct["status_update"] = []
    for line in f:
      if linect == 0:
        ct["time_start"] = getLogTime(line)

      match = dataread.match(line)
      if match:
        datanode = match.group(1)
        ct["datanode"].append(datanode)
        ct["lastDatanode"] = datanode
        if datanode.startswith(SLOWHOST):
            ct["isSlowDatanode"] = True
            master["slowNodeInvolvedInDataread"] = True

      if re_hb.match(line):
        ct["status_update"].append(getLogTime(line))

      if re_container_finished.match(line):
         ct["isSuccessful"] = True
         if ct["ismap"] and SLOWHOST in ct["mapnode"]:
            master["slowNodeInvolvedInMap"] = True
         if not ct["ismap"] and SLOWHOST in ct["reducenode"]:
            master["slowNodeInvolvedInReduce"] = True

      match = re_tags_pbse.match(line)
      if match:
        if match.group(1) not in master["tags_PBSE"]:
          master["tags_PBSE"].append(match.group(1))

      linect += 1

    if line:
      ct["time_stop"] = getLogTime(line)

  # shuffle time
  for ctname,ct in app["containers"].items():
    syslog = os.path.join("yarn/userlogs", \
                            appname,ctname,"syslog.shuffle")
    if not os.path.exists(syslog):
      continue
    shuffleTime = 0.0
    f = open(syslog)
    fmatch = 0
    for line in f:
      match = re_shffle_duration.match(line)
      if match and match.group(2) == lookForShuffleTime:
          shuffleTime = shuffleTime + int(match.group(4))
    ct['shuffleTime'] = (shuffleTime / 1000000000)


def getJobClientStats(apps):
  for root, dirnames, filenames in os.walk('workGenLogs'):
    for fname in filenames:
      jcoutput = os.path.join(root, fname)
      if not os.path.exists(jcoutput):
        return

      f = open(jcoutput)
      jc = {}
      for line in f:
        match = re_jc_appid.match(line)
        if match:
          apps[match.group(1)]["jobclient"] = jc
          jc["time_submit"] = getLogTime(line)

        if re_jc_startrun.match(line):
          jc["time_start"] = getLogTime(line)

        if re_jc_stoprun.match(line):
          jc["time_stop"] = getLogTime(line)

        match = re_jc_duration.match(line)
        if match:
          jc["job_duration"] = int(match.group(1))


def getTopology():
  # get tasks log structure
  apps = {}
  for root, dirnames, filenames in os.walk('yarn/userlogs'):
    if root == "yarn/userlogs":
      for subdirname in dirnames:
        apps[subdirname] = {"master":{},"containers":{}}
    else:
      theroot = root.split(os.path.sep)[-1]
      if theroot in apps:
        ctcount = 0
        for subdirname in sorted(dirnames):
          if not ctcount:
            apps[theroot]["appid"] = theroot
            apps[theroot]["master"]["syslogdir"] = subdirname
          else:
            container = {
              "appid": theroot,
              "containerid": subdirname,
              "attempt": "",
              "mapnode": "",
              "reducenode": "",
              "datanode": [],
              "lastDatanode": "NONE",
              "ismap": False,
              "shuffleTime": 0.0,
              "isSuccessful": False,
              "isSlowMapnode": False,
              "isSlowDatanode": False,
              "isSlowReducenode": False
              }
            apps[theroot]["containers"][subdirname] = container
          ctcount += 1

  # get task topology from MRApp syslog
  for appname,app in apps.items():
    getMasterStats(app)

  # get datanodes from container syslog
  for appname,app in apps.items():
    try:
       getContainerStats(app)
       master = app["master"]
       master["isInvolveSlownode"] = master["slowNodeInvolvedInDataread"] \
         or master["slowNodeInvolvedInMap"] \
         or master["slowNodeInvolvedInReduce"]
    except Exception as e:
       print 'One container failed with : ' + str(e)
  getJobClientStats(apps)

  return apps


def makeCDFPoints(vals):
  X = np.asarray(sorted(vals))
  Y = np.ones(len(vals))
  Y /= Y.sum()
  Y = np.cumsum(Y)
  X = np.insert(X,0,0)
  Y = np.insert(Y,0,0)
  return (X,Y)


def setFigureLabel(fig,suptitle,axtitle,xlabel,ylabel):
  fig.suptitle(suptitle, fontsize=14, fontweight='bold')
  ax = fig.add_subplot(111)
  fig.subplots_adjust(top=0.85)
  ax.set_xlabel(xlabel)
  ax.set_ylabel(ylabel)
  ax.set_title(axtitle)

def printGraphs(apps):
  figs = []

  TASKS = [task for aname,a in apps.items() \
             for tname,task in a["containers"].items()]
  MAPS = [a for a in TASKS if (a["ismap"])]
  REDUCES = [a for a in TASKS if (not a["ismap"])]
  # SLOW_MAPS = [a for a in TASKS if (a["ismap"]) and (SLOWHOST in a["mapnode"])]
  # SLOW_REDUCES = [a for a in TASKS if (not a["ismap"]) and (SLOWHOST in a["reducenode"])]
  AM = [a["master"] for aname,a in apps.items()]
  JC = [a["jobclient"] for aname,a in apps.items() if ("jobclient" in a)]
  ALL_JOBS = [a for aname,a in apps.items()]

  # Heartbeat CDF
  dat = [len(a["status_update"]) for a in MAPS if ("status_update" in a)]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Heartbeat CDF","#map = "+str(len(dat)),"#hb/task","percentage")
  figs.append(fig)
  plt.close()

  # Map Running Time
  dat = [(strToDate(a["time_stop"])-strToDate(a["time_start"])).total_seconds() \
         for a in MAPS if ("time_start" in a) and ("time_stop" in a)]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Map Running Time","#map"+str(len(dat)),"second","percentage")
  figs.append(fig)
  plt.close()

  # Reduce Running Time
  dat = [(strToDate(a["time_stop"])-strToDate(a["time_start"])).total_seconds() \
         for a in REDUCES if ("time_start" in a) and ("time_stop" in a)]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Reduce Running Time","#reduce"+str(len(dat)),"second","percentage")
  figs.append(fig)
  plt.close()

  # Shuffle Running Time
  dat = [a["shuffleTime"] for a in REDUCES if ("shuffleTime" in a) and ("time_start" in a) and ("time_stop" in a)]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Shuffle Running Time","#reduce"+str(len(dat)),"second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time
  dat = [(strToDate(a["time_stop"])-strToDate(a["time_start"])).total_seconds() \
         for a in AM if ("time_start" in a) and ("time_stop" in a)]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Job Running Time (by AM)","#job = "+str(len(dat)),\
                 "second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time for jobs where the slow node is involved in map
  dat = [(strToDate(a["master"]["time_stop"])-strToDate(a["master"]["time_start"])).total_seconds() \
         for a in ALL_JOBS if ("time_start" in a["master"]) and ("time_stop" in a["master"]) and (a["master"]["slowNodeInvolvedInMap"]) and (not a["master"]["slowNodeInvolvedInReduce"])]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Job Running Time (by AM, slow node involved as map only)","#job = "+str(len(dat)),\
                 "second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time for jobs where the slow node is involved in reduce
  dat = [(strToDate(a["master"]["time_stop"])-strToDate(a["master"]["time_start"])).total_seconds() \
         for a in ALL_JOBS if ("time_start" in a["master"]) and ("time_stop" in a["master"]) and (not a["master"]["slowNodeInvolvedInMap"]) and (a["master"]["slowNodeInvolvedInReduce"])]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Job Running Time (by AM, slow node involved as reduce only)","#job = "+str(len(dat)),\
                 "second","percentage")
  figs.append(fig)
  plt.close()

  # Job Running Time for jobs where the slow node is involved in reduce and map
  dat = [(strToDate(a["master"]["time_stop"])-strToDate(a["master"]["time_start"])).total_seconds() \
         for a in ALL_JOBS if ("time_start" in a["master"]) and ("time_stop" in a["master"]) and (a["master"]["slowNodeInvolvedInMap"]) and (a["master"]["slowNodeInvolvedInReduce"])]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Job Running Time (by AM, slow node involved as reduce and map)","#job = "+str(len(dat)),\
                 "second","percentage")
  figs.append(fig)
  plt.close()

  # JobClient Duration
  dat = [a["job_duration"] for a in JC if ("job_duration" in a)]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Job Duration (by JobClient)","#job = "+str(len(dat)),\
                 "second","percentage")
  figs.append(fig)
  plt.close()

  # Job Waiting Time
  dat = [(strToDate(a["time_start"])-strToDate(a["time_submit"])).total_seconds() \
         for a in JC if ("time_start" in a) and ("time_submit" in a)]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Job Waiting Time","#job = "+str(len(dat)),\
                 "second","percentage")
  figs.append(fig)
  plt.close()

  # Locality percentage
  dat = [float(a["ct_HostLocal"])/float(a["ct_CompletedMaps"]) \
         for a in AM if ("ct_HostLocal" in a) and ("ct_CompletedMaps" in a)]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Locality Percentage","#job = "+str(len(dat)),\
                 "% locality","percentage")
  figs.append(fig)
  plt.close()

  # Map Spec Count
  dat = [int(a["ct_SpecMap"]) for a in AM]
  X,Y = makeCDFPoints(dat)
  fig = plt.figure(figsize=(8, 6))
  plt.plot(X, Y, 'r-')
  plt.xlim([0,X.max()*1.1])
  plt.ylim([0.0,1.0])
  setFigureLabel(fig,"Map Speculation per Job","#job = "+str(len(dat)),\
                 "#spec map","percentage")
  figs.append(fig)
  plt.close()


  with PdfPages('all_graphs.pdf') as pdf:
    ct = 0
    for fig in figs:
      print "saving page %d" % ct
      pdf.savefig(fig)
      #fig.savefig("fig-%d.png" % ct)
      ct += 1

def addTopoStats(apps):
  for appname,app in apps.items():
    tasks = []
    d = {}

    for ctname,container in app["containers"].items():
      if container["ismap"]:
        container["topo"] = getPlacementCode(container)
        tasks.append(container)

    for task in tasks:
      tid = getTaskId(task)
      if tid in d:
        d[tid].append(task)
      else:
        d[tid] = [task]

    mapsTopo = []
    for k,v in d.items():
      tcode = getTaskCode(v)
      mapsTopo.append(tcode)
    app["master"]["mapsTopo"] = Counter(mapsTopo)


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


def saveAppsStats(apps):
  with open('data.json', 'w') as fp:
    toprint = {}
    toprint["conf"] = {
      "version": VERSION,
      "style_line":"g-",
      "linewidth": 2
    }
    toprint["apps"] = apps
    json.dump(toprint, fp, indent=2, sort_keys=True)

def main():
  apps = getTopology()
  addTopoStats(apps)
  saveAppsStats(apps)
  printTopoStats(apps)
  printGraphs(apps)


def printContainers():
  tasks = getTopology()
  printTasks(tasks)

if __name__ == '__main__':
  main()
#  printContainers()
