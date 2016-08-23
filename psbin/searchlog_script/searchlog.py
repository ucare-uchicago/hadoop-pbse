#!/usr/bin/python

import os, re, sys
import pprint
import datetime
import json
import argparse
from collections import defaultdict

import numpy as np

import table

import query

ORDERBY = lambda x: x
from query import ORDERBY

pp = pprint.PrettyPrinter(indent=2)

parser = argparse.ArgumentParser(description='Query the log data. Edit query.json to refine your query.')
parser.add_argument("file", help="JSON log data", nargs='?')

args = parser.parse_args()


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
    print "Usage: searchlog.py <json file>"
    sys.exit(1)

  data = loadExpStats(sys.argv[1])

  # querying
  results = [query.SELECT(row) for row in sorted(query.FROM(data['apps']),key=ORDERBY) if query.WHERE(row)]

  # printing
  for res in results:
    st = ", ".join([str(r) for r in res])
    print st


if __name__ == '__main__':  
    main()
