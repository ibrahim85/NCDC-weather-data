
import re,pickle,base64,zlib
import sys
import numpy as np
from StringIO import StringIO
sys.path.append('/usr/lib/python2.6/dist-packages') 
from mrjob.job import MRJob  

class Mean_Readings(MRJob):
    def mapper(self, _, line):
        self.increment_counter('MrJob Counters','mapper',1)
        elements = line.split(',')
        values = np.genfromtxt(StringIO(line[22:]), delimiter=",")
        yield(elements[0],values)
    def reducer(self, station, readings):
        self.increment_counter('MrJob Counters','reducer',1)
        values = np.zeros((1,730))
        count = np.zeros((1,730))
        for each in readings:
            values = np.genfromtxt(StringIO(each), delimiter=",")
            count = count + isfinite(values)
            values[isnan(values)] = 0
            avg = avg + values
        data = (avg/count, count)
        yield(station, pickle.dumps(data)               

if __name__ == '__main__':
    Mean_Readings.run()