TASKS = lambda apps: [task for aname,a in apps.items() \
           for tname,task in a["containers"].items()]
MAPS = lambda apps: [a for a in TASKS(apps) if (a["ismap"])]
JOBS_BY_AM = lambda apps: [a["master"] for aname,a in apps.items()]
JOBS_BY_JC = lambda apps: [a["jobclient"] for aname,a in apps.items() if ("jobclient" in a)]
APPS = lambda apps: [a for aname,a in apps.items()]
JOBS = JOBS_BY_AM
