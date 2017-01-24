# Container class
class Container:
    # in seconds
    containerTime = 0
    isMap = False
    isReduce = False
    startDate = ''
    onSlowNode = False
    isSuccessful = False
    attemptId = ''
    wholeAttemptId = ''
    shuffleFinishTime = ''
    sortFinishTime = ''
    reduceFinishTime = ''

    # added by Riza
    touchSlowNode = False
    firstHeartbeat = ''
    statusUpdate = []
    node = ''
