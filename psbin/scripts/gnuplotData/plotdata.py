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

pp = pprint.PrettyPrinter(indent=2)


NORMALIZE = False
CDF_Y_LIMIT = 1.0

TASKS = lambda apps: [task for aname,a in apps.items() \
           for tname,task in a["containers"].items()]
MAPS = lambda apps: [a for a in TASKS(apps) if (a["ismap"])]
REDUCES = lambda apps: [a for a in TASKS(apps) if (not a["ismap"])]
AM = lambda apps: [a["master"] for aname,a in apps.items()]
JC = lambda apps: [a["jobclient"] for aname,a in apps.items() if ("jobclient" in a)]
ALL_JOBS = lambda apps: [a for aname,a in apps.items()]


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


def makeCDFPoints(vals):
  X = np.asarray(sorted(vals))
  Y = np.ones(len(vals))
  Y /= Y.sum()
  Y = np.cumsum(Y)
#  X = np.insert(X,0,0)
#  Y = np.insert(Y,0,0)
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


def printData(runs, selector, prefix):
  for runid,data in sorted(runs.items(), key=lambda x:x[0]):
    dat = selector(data["apps"])
    X,Y = makeCDFPoints(dat)

    f = open(prefix+"-"+runid+".dat", 'w+')
    for i in xrange(0,len(X)):
      f.write("%f\t%f\n" % (X[i],Y[i]))
    f.close()


def printAllRunData(runs):
  # Job Running Time
  dat = lambda apps: \
        [(strToDate(a["time_stop"])-strToDate(a["time_start"])) .total_seconds() \
         for a in AM(apps) if ("time_start" in a) and ("time_stop" in a)]
  printData(runs,dat,"am")

def printMapTaskCount(runs):
  # Original map task count
  dat = lambda apps: \
	[a["ct_CompletedMaps"] for a in AM(apps)]
  printData(runs,dat,"mapcount")

def printDiff(d1,d2,filename,comp):
  diff = []
  for k,v in d1.items():
#    print k,v,d2[k],comp(v,d2[k])
    val = comp(v,d2[k])
    diff.append(val)
  X,Y = makeCDFPoints(diff)

  f = open(filename, 'w+')
  for i in xrange(0,len(X)):
    f.write("%f\t%f\n" % (X[i],Y[i]))
  f.close()


def printPercentileDiff(d1,d2,filename,comp):
  X1,Y1 = makeCDFPoints(d1.values())
  X2,Y2 = makeCDFPoints(d2.values())
  X = []
  Y = []
  totalData = len(X1)
  for x in xrange(0,len(X1)):
    X.append(100.0 * (x+1) / totalData)
    Y.append(comp(X1[x],X2[x]))
#    print X1[x],X2[x]
#  print "---------"

  f = open(filename, 'w+')
  for i in xrange(0,len(X)):
    f.write("%f\t%f\n" % (X[i],Y[i]))
  f.close()


def printDiffData(json1,json2,prefix):
  dat = lambda apps: \
        [(a["appid"][-3:],(strToDate(a["time_stop"])-strToDate(a["time_start"])) .total_seconds()) \
         for a in AM(apps) if ("time_start" in a) and ("time_stop" in a)]
  d1 = {k:v for k,v in dat(json1["apps"])}
  d2 = {k:v for k,v in dat(json2["apps"])}
  comp = lambda x,y: 100.0 * (x-y) / x
  # comp = lambda x,y: x-y
  printPercentileDiff(d1,d2,"reduct-"+prefix+".dat",comp)

  comp = lambda x,y: x/y
  printPercentileDiff(d1,d2,"speedup-"+prefix+".dat",comp)

  

def loadJson(fname):
  with open(fname) as data_file:
    json_data = json.load(data_file)
    fileversion = json_data["conf"]["version"]
    return json_data


def loadExpStats():
  runs = {}
  for root, dirnames, filenames in os.walk('.'):
    for fname in filenames:
      if fname.endswith(".json"):
        runs[fname.replace(".json","")] = loadJson(fname)
    break
  return runs


def main():
  opt = sys.argv[1]
  if opt == "-a":
    runs = loadExpStats()
    printAllRunData(runs)
  elif opt == "-d":
    run1 = sys.argv[2]
    run2 = sys.argv[3]
    prefix = sys.argv[4]
    json1 = loadJson(run1)  
    json2 = loadJson(run2)
    printDiffData(json1,json2,prefix)
  elif opt == "-c":
    runs = {}
    run1 = sys.argv[2]
    runs[run1.replace(".json","")] = loadJson(run1)
    printMapTaskCount(runs)


if __name__ == '__main__':
  main()
