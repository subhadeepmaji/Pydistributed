
import jobqueues
import threading
import socket
import select 
import sys
import pickle
import customErrors


#server listener thread, forks a worker thread whenever a remote client requests job 
class serverListener(threading.Thread):

    def __init__(self, port, inputJQ, outputJQ):
        
        self.portID = port 
        self.host = ''
        self.server = None
        self.clientthreads = []
        if ((not inputJQ.__class__ is jobqueues.inputJobQueue) or (not outputJQ.__class__ is jobqueues.outputJobQueue)):
            
            raise classNotSupportedError("instance variable not of supported types")
            sys.exit(1)
            
        else:
            self.inputJQ  = inputJQ
            self.outputJQ = outputJQ
        threading.Thread.__init__(self)
        self.start()

    def open_socket(self):

        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.bind((self.host,self.portID))
            self.server.listen(5)
    
        except socket.error, (value,message):
            if self.server:
                self.server.close()
            print "Could not open socket: " + message
            sys.exit(1) 

    def run(self):

        self.open_socket()
   
        while 1:
       
            inputrd,outputrd,exceptrd = select.select([self.server], [], [])
    
            for inputopt in inputrd:
            # Handle New connect request 
                worker = workerThread(self.server.accept(), inputJQ, outputJQ)
                worker.start()
                self.clientthreads.append(worker)
              
            self.server.close()
            for c in self.clientthreads:
                c.join()
     

# Worker thread that designates a job to a remote client       
class workerThread(threading.Thread):

    def __init__(self, (client, address), inputJQ, outputJQ):
        
        self.client = client
        self.address = address
        self.size = 4*1024
        
        # Non-blocking IO send receive
        
        self.client.setblocking(0)
        self.client.settimeout(120)

        if ((not inputJQ.__class__ is jobqueues.inputJobQueue) or (not outputJQ.__class__ is jobqueues.outputJobQueue)):
            
            raise classNotSupportedError("instance variable not of supported types")
            sys.exit(1)
            
        else:
            self.inputJQ  = inputJQ
            self.outputJQ = outputJQ
        
        threading.Thread.__init__(self)
            

    def run(self):
        
        while 1:    
            recvfailed = 0
            try:
                workJob = self.inputJQ.dequeue()
            except jobqueues.QueueAccessError, e:
                print e.args
                continue
            
            workJobDS = pickle.dumps(workJob)
            
            # Check if job could be sent to remote machine in timeout
            
            totalbytes = sys.getsizeof(workJobDS)
            bytessent = 0
            
            # Loop over send to grab all the data, break if any send op takes 
            # more than timeout, if so add the job back to inputJQ and let the client know U failed
            
            try:
                while bytessent < totalbytes:
                    sent = self.client.send(workJobDS[bytessent:])
                    bytessent += sent
                        
            except RuntimeError, e:
                raise socket.error("Send failed")
                self.addFailedJob()    
                break
            
            if (bytestsent < totalbytes):
                raise socket.error("Send failed")
                self.addFailedJob()
                break
            
            # Loop over recv to grab all the data, break if client tells u to break or u waited 
            # more than timeout, if so add the job back to inputJQ 
            
            result = ""
            while resultdataDS:
                try:
                    resultdataDS = self.client.recv(self.size)
                
                # client says hang up !
                except socket.error, (e,messaage):
                    print message
                    self.addFailedJob()
                    recvfailed = 1
                
                # I waited too long !!    
                except socket.timeout, (e,message):
                    print message
                    self.addFailedJob()
                    recvfailed = 1
                    raise socket.error("server timeout")

                result += resultdataDS
            
            if (recvfailed):
                break

            resultdataOBJ = pickle.loads(result)
                
            if (resultdataOBJ):
                self.outputJQ.addToQueueFJ(workJob)
            else:
                self.addFailedJob()
                    
            self.client.close()

    def addFailedJob(self):
        
        while 1:
            try:
                self.inputJQ.enqueue(workJob)
            except jobqueues.QueueAccessError, e:
                print e.args
                continue
            self.client.close()
               

        
    
inputJQ  = jobqueues.inputJobQueue(10)
outputJQ = jobqueues.outputJobQueue()

serverlist = serverListener(4000, inputJQ, outputJQ)


