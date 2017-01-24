import table

''' Search speculative map that read from slow datanode 
FROM = table.MAPS
WHERE = lambda map: not map['attempt'].endswith('_0') and (map['isSlowDatanode'] == True)
SELECT = lambda map: [map['attempt'], map['mapnode'], map['lastDatanode']] '''



''' Search job that (involve slownode and run for more than 80s) or had UCARE_SE algorithm triggered '''
print "appid, durations(s), launch_dr, commit_dr, #maps, #reduce, slowDN, slowMap, slowReduce, slowWrite, ucareSeTags, mapsTopo"
FROM = table.JOBS
#WHERE = lambda job: True
WHERE = lambda job: (job["isInvolveSlownode"])
#WHERE = lambda job: (job["isInvolveSlownode"] and float(job["job_duration"]) > 25) or job["tags_UCARE_SE"]!=[]
#WHERE = lambda job: (job["commit_duration"] > 5)
SELECT = lambda job: [job["appid"][-3:], job["job_duration"], job["launch_duration"], job["commit_duration"], \
                      job["ct_CompletedMaps"], job["ct_CompletedReds"],\
                      job["slowNodeInvolvedInDataread"], job["slowNodeInvolvedInMap"], job["slowNodeInvolvedInReduce"],\
                      job["slowNodeInvolvedInDatawrite"],\
                      " ".join(tag[9:] for tag in job["tags_UCARE_SE"]),\
                      " ".join(['%s:%s' % (key, value) for (key, value) in job["mapsTopo"].items()])]
ORDERBY = lambda job: (job["job_duration"])
#ORDERBY = lambda job: (job["appid"])


''' Search all attempt by task id
print "attempt, containerid, lastDatanode, mapnode, reducenode, topo"
FROM = table.TASKS
#WHERE = lambda task: True
WHERE = lambda task: task["appid"].endswith("_0019")
SELECT = lambda task: [task["attempt"][-10:], task["containerid"][-5:], task["lastDatanode"][:5], task["mapnode"][:5], \
                       task["reducenode"][:5], task["topo"] if "topo" in task else "--", \
                       task["time_start"], \
#                       task["status_update"][0], \
                       task["time_stop"], \
                       " ".join(tag for tag in task["tags_UCARE_SE"])]
ORDERBY = lambda task: task["attempt"] '''
