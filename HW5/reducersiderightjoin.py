from mrjob.job import MRJob
from mrjob.step import MRJobStep
from mrjob.compat import get_jobconf_value
 
class rightjoin(MRJob):
    def mapper(self, _, line):
        x = line.split("|")
        if len(x) == 4:
            yield x[0], ("lefttable", x[1], x[2], x[3])
        else:
            yield x[0], ("righttable", x[1])

    def reducer(self, key, values):
        customers = list()
        orders = list()
        for val in values:
            if val[0]== u'lefttable':
                customers.append(val)
            else:
                orders.append(val)
        for o in orders:
            if len(customers)==0:
                yield None, [key] + [None, None, None] + o[1:]
            for c in customers:
                yield None, [key] + c[1:] + o[1:]

if __name__ == '__main__':
    rightjoin.run()