from mrjob.job import MRJob
from mrjob.step import MRStep

from numpy import mat, zeros, shape, random, array, zeros_like, dot, linalg
from random import sample
import json

import ast

from math import pi, sqrt, exp, pow


class BMM_Maximization_Alpha(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def steps(self):
        return [
                MRStep(mapper_init = self.mapper_init,
                        mapper=self.mapper,
                       mapper_final = self.mapper_final,
                       reducer=self.reducer,
                      reducer_final = self.reducer_final) 
            ]

    
    def __init__(self, *args, **kwargs):
        #call base init method
        super(BMM_Maximization_Alpha, self).__init__(*args, **kwargs)
        
        
        #delete contents of alpha
        fullPath = self.options.pathName + "AlphaResults.txt" 
            
        with open(fullPath,"w") as f:
            pass  #this is to delete contents of file.
                                        
        self.alpha = {}
            
            
                                                 
    def configure_options(self):
        #call base configure_options method
        super(BMM_Maximization_Alpha, self).configure_options()
        
        self.add_passthrough_option(
            '--k', dest='k', default=2, type='int',
            help='k: number of densities in mixture')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="", type='str',
            help='pathName: pathname where AlphaResults.txt is stored')
        
    def mapper_init(self):
        self.N = []
        
    def mapper(self, _, values):
        values = values.split("\t")
        tokens = values[0].replace("]","").replace("[","")
        tokens = tokens.strip()
        doc,k = tokens.split(",")
        r_nk = float(values[1].strip())
        if doc not in self.N: self.N.append(doc)
        #print k.strip(), r_nk
        yield k.strip(), r_nk
        
    def mapper_final(self):
        #print "mapper_final ", len(self.N)
        yield "*",len(self.N)
        
    def reducer(self, k, r_nk):
        numerator = sum(r_nk)
        #print "reducer in", k, numerator
        
        if k == "*":
            self.N = numerator
        else:  #write alphas
            alpha_k = numerator/self.N
            self.alpha[k] = alpha_k
            
    def reducer_final(self):
        with open(self.options.pathName + 'AlphaResults.txt',"a") as f:
            strOutput = "*alpha\t" + str(self.alpha)
            f.write(strOutput+"\n")


    
if __name__ == '__main__':
    BMM_Maximization_Alpha.run()
    