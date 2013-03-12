
import jobqueues
import threading
import socket
import customErrors

class clientWorker(threading.Thread):

    def __init__(self, portToCon, serverAddr):

        self.port = portToCon 
        self.serverAddr = serverAddr
        self.size = 4*1024
        self.connectObj = None
        
        threading.Thread.__init__(self)
        self.start()
        
    def server_connect(self):

        try:
            self.connectObj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connectObj.connect((self.serverAddr, self.port))
        except socket.error, (value,message):
            if self.connectObj:
                self.connectObj.close()
            print "Could not open socket: " +message
            sys.exit(1)
            
        # Non-blocking IO send receive
        
        self.connectObj.setblocking(0)
        self.connectObj.settimeout(120)


    def run(self):
        
        self.server_connect()
        while 1:
            
            jobInsObjDS = ""
            inputdata = True

            while inputdata:   
                try:
                    inputdata = self.connectObj.recv(self.size)
                    jobInsObjDS += inputdata
                except socket.error, (e,message):
                    print message
                    self.connectObj.close()
                    

            jobInsObj = pickle.loads(jobInsObjDS)    
            
            if (not jobInsObj.__class__ is jobqueues.jobInstance):
                print 'Unsupported Object Type: Operation Aborted'
            else:
                #send response to server after executing code
                
                try:
                    jobInsObj.execFunconData()
                except CodeNotcompiledError, e:
                    print e.args
                    raise socket.error(e.args)
                    self.connectObj.close()
                except codeExecutionError, e:
                    print e.args
                    raise socket.error(e.args)
                    self.connectObj.close()
                    
                responsedata = pickle.dumps(jobInsObj.resultSet)
                responsedatasize = sys.getsizeof(responsedata)
                bytessent = 0 
                
                try: 
                    while bytessent < responsedatasize:
                        sent = self.connectObj.send(responsedata[bytessent:])
                        bytessent += sent
                
                #server says hang up !
                except socket.error, (e,message):
                    print message
                    
                # I ran into a problem !
                except RuntimeError, e:
                    raise socket.error("Data not sent fully")
                finally:
                    self.connectObj.close()
                    
                    













        
