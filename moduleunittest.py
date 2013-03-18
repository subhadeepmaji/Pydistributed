
import include
import unittestcode

# These set of module counts the letter frequency in a corpus in distributed manner

def buildwordlist(filename):
    
    corpusfile = open('testtext1.txt',"r")
    wordlist = []

    lines = corpusfile.readlines()
    
    for line in lines:
   
        words = line.split(" ")
        for word in words:
            wordlist.append(word) 
    
    return wordlist  
    

def mapjobs(wordlist, server):
    
    jobdatalists = []
    
    s = 0
    joblen = len(wordlist) / 5
    counter = 0

    while s + joblen < len(wordlist):
        
        jobdatalists[counter] = []
        jobdatalists[counter] = wordlist[s:joblen]
        counter++
        s += joblen
   
    jobdatalists[counter] = wordlist[s:]

    program = open('histogramcode.py',"r")
    
    func = program.readlines()
    
    for data in jobdatalists:
        
        j = jobInstance(1, c, len(jobdatalists))
        j.setDataandFunc(data, func, 'histogram')
        
        server.jobqueue.enqueue(j)
        c++
        
 
def initserver():
    
    s_inputQ  = inputJobQueue(10)
    s_outputQ = outputJobQueue(10)
    
    s = serverListener(4500, s_inputQ, s_outputQ)
    return s


def reduceJobs(server, jobID, NOP):

    return server.outputJQ.resultsetforjobID(jobID,NOP)

       




