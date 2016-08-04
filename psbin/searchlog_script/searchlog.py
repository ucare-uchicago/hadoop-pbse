#!/usr/bin/python

import os, re, sys
import pprint
import datetime
import json
from collections import defaultdict

import numpy as np

pp = pprint.PrettyPrinter(indent=2)


################### DATA TABLE BEGIN ################

# TODO: move this to separate script table.py

TASKS = lambda apps: [task for aname,a in apps.items() \
           for tname,task in a["containers"].items()]
MAPS = lambda apps: [a for a in TASKS(apps) if (a["ismap"])]
JOBS_BY_AM = lambda apps: [a["master"] for aname,a in apps.items()]
JOBS_BY_JC = lambda apps: [a["jobclient"] for aname,a in apps.items() if ("jobclient" in a)]

################### DATA TABLE END   ################


################### QUERY BEGIN ###################

# TODO: move this to separate script query.py
# Haryadi only need to edit query.py to able to see the data

FROM = MAPS
WHERE = lambda map: not map['attempt'].endswith('_0') and map['datanode'] != []
SELECT = lambda map: (map['attempt'], map['mapnode'], map['datanode'][-1])

# Explanation: Select attempt_id, mapnode, and last datanode acessed
# from all map tasks where it's not the original map task and has
# start reading from some datanode

################### QUERY END   ###################


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
  data = loadExpStats(sys.argv[1])

  # querying
  results = [SELECT(row) for row in FROM(data['apps']) if WHERE(row)]
  
  # printing
  for res in results:
    print res


if __name__ == '__main__':
  main()
