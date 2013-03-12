Pydistributed
=============

A set of pyhton modules to exceute a program in a distributed enviroment coordinated by a server

Purpose : To exceute a python program across a network of connected machines, where a single server 
          distributes the program (and asscoiated data) across various worker machines. The workers
          execute the jobs assigned by server and return the result sets, which the server merges to 
          the final result set of the program.

The module needs the worker machines job programs to follow a specific syntax:

def funcMain(data):
...
...
...
[Any valid pyhton code]

Therefore the worker machines enter the assigned job code by excecuting the funcMain
function in the code.

