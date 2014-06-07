#!/usr/bin/python
"""
collect the statistics for each station.
"""
import re,pickle,base64,zlib
import numpy as np
from sys import stderr
import sys

sys.path.append('/usr/lib/python2.6/dist-packages') # a hack because anaconda made mrjob unreachable
from mrjob.job import MRJob
from mrjob.protocol import *

import traceback
from functools import wraps
from sys import stderr
import datetime
def loads(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))

def dumps(Value):
    """ Encode a value as a string """
    return base64.b64encode(zlib.compress(pickle.dumps(Value),9))

meas_choosen = ['TEMP','TMAX', 'TMIN', 'PRCP', 'SNDP']
class Collect_GSODStats(MRJob):

    def only_mapper(self, _, line):
        elements=line.split(',')
        yr, mnth, day = int(elements[2][:4]), int(elements[2][-4:-2]), int(elements[2][-2:])
        if not (mnth ==2 and day == 29): #ignore Feb. 29 data from leap years
            DayOfYear = datetime.date(2014, mnth, day).strftime("%j") #choosing an arbitrary non-leap year
            yield((elements[0], yr),[int(DayOfYear), [elements[2]]+ elements[-5:-1]])
           
          
    def first_reducer(self, (stn,yr), vectors):
        D = np.zeros((len(meas_choosen), 365) ,dtype = int)
        for (day, meas) in vectors:
            for (i, val) in enumerate(meas):
                if(val !='999.9'): D[i,day-1] = 1 
        counts = np.sum(D, axis = 1)
        yield(stn, [yr, counts.tolist()])
  
    def second_reducer(self, stn, vectors):
        D = {}
        for (yr, counts) in vectors:
            for i in range(len(meas_choosen)):
                D[(yr, meas_choosen[i])] = counts[i]
        yield(stn, dumps(D))
  
    def steps(self):
        return [
            self.mr(mapper=self.only_mapper,
                    reducer=self.first_reducer),
            self.mr(reducer=self.second_reducer)
        ]     
if __name__ == '__main__':
    Collect_GSODStats.run()