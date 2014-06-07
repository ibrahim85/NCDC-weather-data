
#!/usr/bin/python
"""
collect the statistics for each station.
"""
import re,pickle,base64,zlib
from sys import stderr
import sys

sys.path.append('/usr/lib/python2.6/dist-packages') # a hack because anaconda made mrjob unreachable
from mrjob.job import MRJob
from mrjob.protocol import *

from sys import stderr


def loads(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))

def dumps(Value):
    """ Encode a value as a string """
    return base64.b64encode(zlib.compress(pickle.dumps(Value),9))


class Collect_GHCNStats(MRJob):
    
    def mapper(self, _, line):
        elements=line.split(',')
        if(elements[1]!='year'):
            yield(elements[0],elements[1:])
            
    def check_integrity(self,meas,year,length):
        if year<1000 or year > 2014: return False
        if meas=='': return False
        if length != 367: return False
        return True
    
    def reduce_one(self,S,vector):
        meas=vector[0]
        year=int(vector[1])
        length=len(vector)
        number_defined=sum([e!='' for e in vector[2:]])
        assert self.check_integrity(meas,year,length)==True
        S[(year, meas)]=number_defined
        
    def reducer(self, station, vectors):
        S={}
        for vector in vectors:
            self.reduce_one(S,vector)
        yield(station,dumps(S))
                              
if __name__ == '__main__':
    Collect_GHCNStats.run()