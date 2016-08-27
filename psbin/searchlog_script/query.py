import table

# Search speculative map that read from slow datanode 
# FROM = table.MAPS
# WHERE = lambda map: not map['attempt'].endswith('_0') and (map['isSlowDatanode'] == True)
# SELECT = lambda map: [map['attempt'], map['mapnode'], map['lastDatanode']]

# Search job latency of job that involve slow node
print "appid, duration(s), #maps, #reduce, slowDN, slowMap, slowReduce, mapsTopo"
FROM = table.JOBS
WHERE = lambda job: job["isInvolveSlownode"] and float(job["job_duration"]) > 180
SELECT = lambda job: [job["appid"][-3:], job["job_duration"], job["ct_CompletedMaps"], job["ct_CompletedReds"],\
                      job["slowNodeInvolvedInDataread"], job["slowNodeInvolvedInMap"], job["slowNodeInvolvedInReduce"],\
                      " ".join(['%s:%s' % (key, value) for (key, value) in job["mapsTopo"].items()])]
ORDERBY = lambda job: (job["job_duration"])
