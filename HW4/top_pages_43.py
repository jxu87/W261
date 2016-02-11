## top_pages_43.py
## Author: Angela Gunn & Jing Xu
## Description: Find 5 most frequently visited pages from the log

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class TopPages(MRJob):
    
    top5 = {} #initialize top5 dictionary

    def steps(self):
        return [MRStep(mapper = self.mapper,
                    combiner = self.combiner,
                    reducer = self.reducer),
                MRStep(reducer = self.output_find_top_5)]

    def mapper(self, line_no, line):
        #Extracts the Vroot that was visited
        line = line.strip(' ')
        cell = csv_readline(line)
        yield cell[1],1 

    def combiner(self, vroot, visit_counts):
        #combines the visits
        total = sum(visit_counts)
        yield vroot, total
        
    def reducer(self, vroot, visit_counts): #Sumarizes the visit counts by adding them together.
        #combines the visits, and adds vroot to top5 dictionary if qualified
        total = sum(visit_counts)
        if len(self.top5) < 5:            #less than 5 items, so add
            self.top5[vroot] = total
        else:    
            #must find the smallest item; if smaller than the new item, delete it and add new item.
            top_min = min(self.top5, key=self.top5.get)
            if total >= self.top5[top_min]:
                del self.top5[top_min]
                self.top5[vroot] = total
        yield vroot, total
    #end def reducer        
    
    def output_find_top_5(self, vroot, visit_counts):
        #outputs the results of our top 5
        if len(self.top5) > 0:
            top_max = max(self.top5, key=self.top5.get)
            yield top_max, self.top5.pop(top_max)

if __name__ == '__main__':
    TopPages.run()