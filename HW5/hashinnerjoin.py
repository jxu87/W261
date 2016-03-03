from mrjob.job import MRJob
from mrjob.step import MRJobStep
from mrjob.compat import get_jobconf_value
 
class leftjoin(MRJob):

    urls = {}
    with open('/Users/JingXu/Dropbox/DataScience/W261/W261/HW5/url.txt','r') as f:
        for line in f: 
            cell = csv_readline(line)
            if cell[1] not in urls.keys():
                self.urls[cell[1]] = cell[4]
        
    def mapper(self, _, line):


    def mapper_final(self):
        yield urls

if __name__ == '__main__':
    leftjoin.run()