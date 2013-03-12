
import threading
import Queue
import re
import customErrors

# A single job to be exceuted on a remote machine
class jobInstance(object):

    def __init__(self, jID, pID, NP):
        self.jobID = jID
        self.partID = pID
        self.NOP = NP
        self.resultSet = list()     
   
    def setDataandFunc(self, data, func, mainFunName):
        self.data = data
        self.func = func
        self.mnName = mainFunName

    # Exceute a python block of code 
    def execFunconData(self):
        
        funcnameprefix = 'code_object_%d' %self.jobID
        funcname = 'def %s' %self.mnName
        funcnewname = 'def %s%s'%(funnameprefix,self.mnName)
        
        re.sub(funcname, funcnewname, self.func)
        
        self.code_object = compile(self.func, '', 'exec')

        try:
            exec self.code_object
        except RuntimeError, e:
            raise CodeNotcompiledError("Code could not be compiled")
        
        funcname = funcnameprefix + self.mnName
        try:
            self.resultSet = funcname(self.data)
        except RuntimeError, e:
            raise codeExecutionError("Code execution failed")


# A queue of jobs read by remote worker threads and written by master
class inputJobQueue():

    def __init__(self, queuesize):
        self.QS = queuesize
        self.workQueue = Queue.Queue(self.QS)
        self.queueLock = threading.Lock()
           
    def enqueue(self, jobInstance_obj):
    
       self.queueLock.acquire()
     
       if (not self.workQueue.full()):
            self.workQueue.put(jobInstance_obj)
       else:
            raise QueueAccessError("QUEUE FULL")
       self.queueLock.release()
   
    def dequeue(self):
       
       self.queueLock.acquire()
    
       if (not self.workQueue.empty()):
           return self.workQueue.get()
       else:
           raise QueueAccessError("QUEUE EMPTY")
       self.queueLock.release()    


class outputJobQueue():

    def __init__(self):
    
        self.jobLock = threading.Lock()
        self.finishedjobs = dict()

    # Add a finished job to finished jobs dict
    def addToQueueFJ(self, jobInstance):
    
        self.jobLock.acquire()
        if (not self.finishedjobs[jobInstance.jobID, jobInstance.NOP]):
            self.finishedjobs[jobInstance.jobID, jobInstance.NOP] = [] 
         
        partIDlist = map(lambda jobIns : jobIns.partID, self.finishedjobs[jobInstance.jobID, jobInstance.NOP])


        if (jobInstance.partID not in partIDlist):
            self.finishedjobs[jobInstance.jobID, jobInstance.NOP].append(jobInstance)
        
        if (len(self.finishedjobs[jobIsntance.jobID, jobInstance.NOP]) == jobInstance.NOP):
            
            for jobIns in self.finishedjobs[jobInstance.jobID, jobInstance.NOP]:
                partIDdict[jobIns.partID] = jobIns
            for key in sorted(partIDdict.iterkeys()):
                print partIDdict[key].resultSet         

            self.finishedjobs[jobInstance.jobID, jobsInstance.NOP] = []
        
        self.jobLock.release()         

    
        
