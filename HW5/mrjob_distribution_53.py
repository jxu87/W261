from mrjob.job import MRJob
from mrjob.step import MRJobStep
from mrjob.compat import get_jobconf_value

class distribution_53(MRJob):
    ngram_count = 0
    
    def steps(self):
        return [MRJobStep(mapper=self.mapper,
                         reducer=self.reducer,
                         reducer_final = self.reducer_final)]
        
    def mapper(self, _, line):
        #this is the logs
        cell = line.split('\t')
        ngram = cell[0].lower()
        yield (ngram, cell[1]), 1
        
    def reducer(self, key, values):
        #
        total = sum(values)
        ngram, count = key
        print total
        self.ngram_count += total
        print self.ngram_count
        yield ngram, int(count)
                       
    def reducer_final(self, key, value):
        total = sum(value)
        dist = float(total)/self.ngram_count
        yield key, dist
            
        

    #end def reducer

        
if __name__ == '__main__':
    distribution_53.run()