from mrjob.job import MRJob
from mrjob.step import MRJobStep
from mrjob.compat import get_jobconf_value

class density_53(MRJob):

    
    def steps(self):
        return [MRJobStep(mapper=self.mapper,
                         reducer=self.reducer
                         ),
               MRJobStep(mapper=self.mapper_sort,
                        reducer=self.reducer_sort)]
        
    def mapper(self, _, line):
        #this is the logs
        cell = line.split('\t')
        words = cell[0].split()
        #freq = float(cell[1])/ int(cell[2])
        for w in words:
            yield w.lower(), [cell[1],cell[2]]
        
    def reducer(self, key, value):
        #
        values = list(value)
        
        density = 0
        count = sum(int(count) for count,pages in values)
        pages = sum(int(pages) for count,pages in values)
        yield count*1.0/pages, key
        
    def mapper_sort(self, key, values):
        yield key, values
        
    def reducer_sort(self, key, values):
        word = list(values)
        yield word[0], key
            
        

    #end def reducer

        
if __name__ == '__main__':
    density_53.run()