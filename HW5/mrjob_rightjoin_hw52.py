#!/usr/bin/python
## mrjob_rightjoin_hw52.py
## Author: Angela Gunn  & Jing Xu
## Description:right Join
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value

import csv

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row


class rightjoin(MRJob):
    def steps(self):
        return [MRStep(mapper_init = self.mapper_init,
                     mapper=self.mapper)]
    
    def mapper_init(self):
        #store the URLs
        self.lefttable = {}
        with open('url.txt') as f:
            for line in f:
                cell = csv_readline(line)
                self.lefttable[cell[1]] = cell[4]
        
    def mapper(self, _, line):
        #this is the logs
        cell = csv_readline(line) 
        if cell[1] in self.lefttable.keys():
            yield cell[1], (self.lefttable[cell[1]], cell[3]) #yield the matching rows.     
        else:
            yield None, (None , cell[3])
        
if __name__ == '__main__':
    rightjoin.run()