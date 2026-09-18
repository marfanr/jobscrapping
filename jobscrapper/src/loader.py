REGISTERED_JOBS = {}
REGISTERED_OLAP_TASKS = {}

def registerJob(func):
    def wrapper(obj):
        REGISTERED_JOBS[func] = obj
    return wrapper

def getJobs():
    return REGISTERED_JOBS

def registerTask(func):
    def wrapper(obj):
        REGISTERED_OLAP_TASKS[func] = obj
    return wrapper

def getTask(name: str):
    if name not in REGISTERED_OLAP_TASKS:
        raise "task not exists"
    return REGISTERED_OLAP_TASKS[name]