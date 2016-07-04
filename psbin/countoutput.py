#!/usr/bin/python

import os, re, sys
import pprint
from collections import defaultdict

# slownode: node-1
SLOWHOST="pc222"
SLOWIP="10.1.1.3"

pp = pprint.PrettyPrinter(indent=2)

pt = re.compile('.+([FSn][fsn]_[FSn][fsn])=([0-9]+)')
verd = re.compile('.+(ORIGINAL_WIN|BACKUP_WIN)=([0-9]+)')

class JobCount:
  def __init__(self, filename):
    outfile = open(filename)
    self.jobct = defaultdict(int)
    self.verdct = defaultdict(int)

    self.jobcountfound = False

    for line in outfile:
      m = pt.match(line)
      if m:
        self.jobct[m.group(1)] += int(m.group(2))
        self.jobcountfound = True

      m = verd.match(line)
      if m:
        self.verdct[m.group(1)] += int(m.group(2))

    if not self.jobcountfound:
      print "WARNING: no counter for " + filename


def getJobs():
  jobs = []
  fct = 0
  for root, dirnames, filenames in os.walk('.'):
    for inf in sorted(filenames):
      if inf.startswith("job-"):
        fct += 1
        jc = JobCount(inf)
        if (jc.jobcountfound):
          jobs.append(jc)
  print "%d/%d job output read" % (len(jobs),fct)
  return jobs

def getJobCount(jobs):
  ct = defaultdict(int)
  for job in jobs:
    for k,v in job.jobct.items():
      ct[k] += v
  return ct

def getVerdCount(jobs):
  ct = defaultdict(int)
  for job in jobs:
    for k,v in job.verdct.items():
      ct[k] += v
  return ct

def main():
  jobs = getJobs()

  ct = getJobCount(jobs)
  total = 0
  it = sorted(ct.items(),key=lambda x:x[0])

  print "Count:"
  total = 0
  for k,v in it:
    print "%s = %d" % (k,v)
    total += v
  print "Total: ", total
  print ""

  print "Percent:"
  for k,v in it:
    print "%s = %.0f" % (k,float(v)/total*100)
  print ""

  print "Verd Count:"
  total = 0
  for k,v in getVerdCount(jobs).items():
    print "%s = %d" % (k,v)
    total += v
  print "Total: ", total
  print ""

if __name__ == '__main__':
  main()
