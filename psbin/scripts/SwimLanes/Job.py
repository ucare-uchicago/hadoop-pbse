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

   # added by Riza
   launchDuration = 0
   commitDuration = 0
