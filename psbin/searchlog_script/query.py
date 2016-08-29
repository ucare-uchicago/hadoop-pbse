import table

''' Search speculative map that read from slow datanode 
FROM = table.MAPS
WHERE = lambda map: not map['attempt'].endswith('_0') and (map['isSlowDatanode'] == True)
SELECT = lambda map: [map['attempt'], map['mapnode'], map['lastDatanode']] '''



''' Search job that (involve slownode and run for more than 80s) or had PBSE algorithm triggered
print "appid, duration(s), #maps, #reduce, slowDN, slowMap, slowReduce, pbseTags, mapsTopo"
FROM = table.JOBS
WHERE = lambda job: (job["isInvolveSlownode"] and float(job["job_duration"]) > 80) or job["tags_PBSE"]!=[]
SELECT = lambda job: [job["appid"][-3:], job["job_duration"], job["ct_CompletedMaps"], job["ct_CompletedReds"],\
                      job["slowNodeInvolvedInDataread"], job["slowNodeInvolvedInMap"], job["slowNodeInvolvedInReduce"],\
                      " ".join(tag[5:] for tag in job["tags_PBSE"]),\
                      " ".join(['%s:%s' % (key, value) for (key, value) in job["mapsTopo"].items()])]
ORDERBY = lambda job: (job["job_duration"]) '''



''' Search all attempt by task id '''
FROM = table.TASKS
WHERE = lambda task: task["appid"].endswith("_0051")
SELECT = lambda task: [task["attempt"], task["lastDatanode"], task["mapnode"], \
                       task["reducenode"], task["topo"] if "topo" in task else "--"]
ORDERBY = lambda task: task["attempt"]
