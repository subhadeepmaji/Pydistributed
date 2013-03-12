
class classNotSupportedError(RuntimeError):
    
    def __init__(self,args):
        self.args = args

class QueueAccessError(RuntimeError):

    def __init__(self, arg):
        self.args = arg

class CodeNotcompiledError(RuntimeError):

    def __init__(self, arg):
        self.args = arg

class CodeExecutionError(RuntimeError):

    def __init__(self, arg):
        self.args = arg
        
        
        


