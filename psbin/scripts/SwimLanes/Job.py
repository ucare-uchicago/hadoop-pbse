# Job class
class Job:
   # in seconds
   jobDurationAM = 0
   jobDurationJC = 0
   jobId = ''
   jobStart = ''
   # list of containers
   containers = []
   # killed by slowShuffle
   containersKilledBySlowShuffle = []
   # slow shuffle detection time
   slowShuffleDetectionTime = []
   # launched by write diversity
   launchedByWriteDiversity = []
   # write diversity detection time
   writeDiversityDetectionTime = []

   # added by Riza
   launchDuration = 0
   commitDuration = 0
