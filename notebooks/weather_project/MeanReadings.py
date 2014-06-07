
#!/usr/bin/python
"""To find mean min and max temp for each station"""
import re,pickle,base64,zlib
import sys
import numpy as np
from StringIO import StringIO
sys.path.append('/usr/lib/python2.6/dist-packages') 
from mrjob.job import MRJob  

class MeanReadings(MRJob):
    
    def mapper(self, _, line):
        self.increment_counter('MrJob Counters','mapper',1)
        elements = line.split(',')
        #values = np.genfromtxt(StringIO(line[22:]), delimiter=",")
        yield(elements[0],line[22:])
        
    def reducer(self, station, readings):
        readings = list(readings)
        self.increment_counter('MrJob Counters','reducer',1)
        avg = np.zeros((1,730))
        avg_var = np.zeros((1,730))
        count = np.zeros((1,730),dtype=float)
        values = np.zeros((1,730))
        for each in readings:
            v = each.strip().split(',') 
            for i in range(len(v)):
                if v[i]=='': values[0,i] = np.nan
                else: values[0,i] = int(v[i])
            count = count + np.isfinite(values)
            values[np.isnan(values)] = 0
            avg = avg + values
        avg = avg/count
        for each in readings:
            v = each.strip().split(',') 
            for i in range(len(v)):
                if v[i]=='': values[0,i] = 0
                else: values[0,i] = int(v[i])
            avg_var = avg_var + np.square(values-avg)
        avg_var = np.sqrt(avg_var/count)
        data = (avg, avg_var, count)
        yield(station, pickle.dumps(data))               

if __name__ == '__main__':
    MeanReadings.run()