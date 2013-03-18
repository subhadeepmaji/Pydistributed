
import include

def histogram(data):

    freqlist = dict()
    
    for word in data:
        
        for char in word:
            
            if (not char in freqlist):
                freqlist[char] = 1
            else:
                freqlist[char] += 1
     
     return freqlist

           


