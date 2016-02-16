## top_visitor_44.py
## Author: Angela Gunn & Jing Xu
## Description: Find most frequent visitor for each page from the log

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class TopVisitor(MRJob):
    
    top_page_visitor = {}
    
    def steps(self):
        return [MRStep(mapper = self.mapper,
                    combiner = self.combiner,
                    reducer = self.reducer),
                MRStep(reducer = self.reducer_frequent_visitor)]

    def mapper(self, line_no, line):
        #Extracts the Vroot that was visited
        line = line.strip(' ')
        cell = csv_readline(line)
        yield (cell[1],cell[3]),1 

    def combiner(self, key, visit_counts):
        #combines the visits
        total = sum(visit_counts)
        yield key, total
        
    def reducer(self, key, visit_counts): #Sumarizes the visit counts by adding them together.
        #combines the visits, and adds the key to top_page_visitor dictionary if qualified
        total = sum(visit_counts)
        page = key[0]
        visitor = key[1][1:]
        top_count = int(self.top_page_visitor.get(page,(visitor,0))[1]) #assign top_count value
        if top_count < total:
            self.top_page_visitor[page] = (visitor,total)        
        yield page, total    
    #end def reducer        
    
    def reducer_frequent_visitor(self, page, visit_counts):
        with open('url.txt','r') as f:
            for line in f:
                cell = csv_readline(line)
                if cell[1] == page:
                    key = "{0:>4}|{1:>5}|".format(page,self.top_page_visitor[page][0]) 
                    break
        yield key, self.top_page_visitor[page][1]

if __name__ == '__main__':
    TopVisitor.run()