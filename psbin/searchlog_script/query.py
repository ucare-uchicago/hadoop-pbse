import table

# Search speculative map that read from slow datanode 
# FROM = table.MAPS
# WHERE = lambda map: not map['attempt'].endswith('_0') and (map['isSlowDatanode'] == True)
# SELECT = lambda map: [map['attempt'], map['mapnode'], map['lastDatanode']]

# Search job latency of job that involve slow node
FROM = table.JOBS
WHERE = lambda job: job["isInvolveSlownode"]
SELECT = lambda job: [job["appid"], job["job_duration"], job["ct_CompletedMaps"], job["ct_CompletedReds"]]
