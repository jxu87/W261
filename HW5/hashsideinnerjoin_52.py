#!/usr/bin/python
## mrjob_join_hw52.py
## Author: Angela Gunn 
## Description:Inner Join
from mrjob.job import MRJob
from mrjob.step import MRJobStep
from mrjob.compat import get_jobconf_value

import csv

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class innerjoin(MRJob):
    def steps(self):
        return [MRJobStep(mapper_init = self.mapper_init,
                     mapper=self.mapper)]
    
    def mapper_init(self):
        #store the URLs
        self.urls = {}
        with open('url.txt') as f:
            for line in f:
                cell = csv_readline(line)
                self.urls[cell[1]] = cell[4]
        
    def mapper(self, _, line):
        #this is the logs
        cell = csv_readline(line) 
        yield cell[1], (self.urls[cell[1]], cell[3]) #yield the matching rows.

if __name__ == '__main__':
    innerjoin.run()