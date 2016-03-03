#!/usr/bin/python
## hashsideleftjoin_52.py
## Author: Angela Gunn & Jing Xu 
## Description:Left Join

from mrjob.job import MRJob
from mrjob.step import MRJobStep
from mrjob.compat import get_jobconf_value
import csv

def csv_readline(line):
    for row in csv.reader([line]):
        return row

class leftjoin(MRJob):
    
    def steps(self):
        return [MRJobStep(mapper_init = self.mapper_init,
                         mapper = self.mapper, mapper_final = self.mapper_final)]
    
    def mapper_init(self):
        self.urls = {} #initialize urls library to 
         
        with open('url.txt') as f:
            for line in f: 
                cell = csv_readline(line)
                self.urls[cell[1]] = [cell[4],[]] #url, list of visitors

    def mapper(self, _, line):
        #these are the logs
        cell = csv_readline(line)
        key = cell[1]
        self.urls[key][1].append(cell[3])

    def mapper_final(self):
        for key, values in self.urls.iteritems():
            url = values[0]
            if len(values[1]) > 0:
                for u in values[1]: yield key, (url, u)
            else:
                yield key, (url, "NONE")

if __name__ == '__main__':
    leftjoin.run()