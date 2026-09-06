REGISTERED_JOBS = {}

def registerJob(func):
    def wrapper(obj):
        REGISTERED_JOBS[func] = obj
    return wrapper

def getJobs():
    return REGISTERED_JOBS