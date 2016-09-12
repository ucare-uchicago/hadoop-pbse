#!/usr/bin/python

import os, re, sys
import pprint
import datetime
import json
import argparse
from collections import defaultdict

import numpy as np

pp = pprint.PrettyPrinter(indent=2)

parser = argparse.ArgumentParser(description='Inspect topology of json file.')
parser.add_argument("file", help="JSON log data", nargs='?')

args = parser.parse_args()


TASKS = lambda app: [task for tname,task in app["containers"].items()]
MAPS = lambda app: [a for a in TASKS(app) if (a["ismap"])]
REDUCES = lambda app: [a for a in TASKS(app) if not (a["ismap"])]
JOBS_BY_AM = lambda app: app["master"]
JOBS_BY_JC = lambda apps: app["jobclient"]
JOBS = JOBS_BY_AM

def getTaskId(ct):
  att = ct["attempt"]
  return att[8:35]  

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


def loadExpStats(filename):
  with open(filename) as data_file:
    data = json.load(data_file)
    return data

def main():
  try:
    arg1 = sys.argv[1]
  except IndexError:
    print "Usage: topo.py <json file>"
    sys.exit(1)

  data = loadExpStats(sys.argv[1])

  for appname, app in data["apps"].items():
    master = app["master"]
    if master["isInvolveSlownode"]:
      print appname, master["job_duration"]

      if master["slowNodeInvolvedInDataread"] or ["slowNodeInvolvedInMap"]:
        print "Detected slow map topology:"
        for map in MAPS(app):
          if map["isSlowDatanode"] or map["isSlowMapnode"]:
            print "%s: %s -> %s" % (map["attempt"], map["lastDatanode"], map["mapnode"])

      if master["slowNodeInvolvedInDatawrite"] or ["slowNodeInvolvedInReduce"]:
        print "Detected slow reduce topology:"
        for red in REDUCES(app):
          if red["isSlowPipeline"] or red["isSlowReducenode"]:
            lastPipe = [] if red["dnpipeline"]==[] else red["dnpipeline"][-1]
            print "%s: %s -> %s" % (red["attempt"], red["reducenode"], lastPipe)

      slowShuffleDetected = False
      for map in MAPS(app):
        for red in REDUCES(app):
          if (map["isSlowMapnode"] or red["isSlowReducenode"]):
            if not slowShuffleDetected:
              slowShuffleDetected = True
              print "Detected slow shuffle topology"
            print "%s : %s -> %s : %s" % (map["attempt"], map["mapnode"], red["reducenode"], red["attempt"])

      print ""      


if __name__ == '__main__':  
    main()
