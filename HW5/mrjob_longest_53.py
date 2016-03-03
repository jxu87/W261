from mrjob.job import MRJob
from mrjob.step import MRJobStep
from mrjob.compat import get_jobconf_value

class longest_ngram(MRJob):
    long_ngram = None
    long_length = 0
    
    def steps(self):
        return [MRJobStep(mapper=self.mapper,
                         reducer=self.reducer,
                         reducer_final=self.reducer_final,
                        jobconf={
                            "mapred.map.tasks":4,
                            "mapred.reduce.tasks":1,
                            })]
        
    def mapper(self, _, line):
        #break out the lengths of the cells
        cell = line.split('\t')
        length = len(cell[0])
        yield cell[0], length
        
    def reducer(self, key, value):
        #Add to global values if largest 
        value = list(value)
        if sum(value) > self.long_length:
            self.long_ngram = key
            self.long_length = sum(value)
            
    def reducer_final(self):
        #output largest ngram and length
        yield self.long_ngram, self.long_length
        
if __name__ == '__main__':
    longest_ngram.run()