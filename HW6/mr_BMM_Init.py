from mrjob.job import MRJob
from mrjob.step import MRStep

from numpy import mat, zeros, shape, random, array, zeros_like, dot, linalg
from random import sample
import json
from math import pi, sqrt, exp, pow


class BMM_Init(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def steps(self):
        return [
                MRStep(mapper_init = self.mapper_init,
                        mapper=self.mapper,
                       mapper_final = self.mapper_final,
                       reducer=self.reducer),
                MRStep(mapper = self.mapper_vocabulary,
                       reducer=self.reducer_vocabulary,
                      reducer_final = self.reducer_vocabulary_final)
            ]

    
    def __init__(self, *args, **kwargs):
        #call base init method
        super(BMM_Init, self).__init__(*args, **kwargs)
        
        #self.numMappers = 1     #number of mappers
        self.count = 0
        self.qmk = {}
        self.qmk_word = ""
        self.key_docs = {}
        
                                                 
    def configure_options(self):
        #call base configure_options method
        super(BMM_Init, self).configure_options()
        
        self.add_passthrough_option(
            '--k', dest='k', default=2, type='int',
            help='k: number of densities in mixture')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="", type='str',
            help='pathName: pathname where WordResults.txt is stored')
    
    def mapper_init(self):
        self.doc_list = []
        
    def mapper(self, _, values):
        values = values.split("\t")
        doc = values[0].strip()
        words = values[1].split()
        self.doc_list.append(doc)
        yield doc, words
            
    def mapper_final(self):
        #pick the seeds
        #self.index = sample(range(len(self.doc_list)), self.options.k)
        self.index = [6-1,7-1] #force docs 6, 7 for testing
        print "seed documents = ", self.index
        k = 1
        for i in self.index:
            yield "*"+self.doc_list[i], k
            k+=1
        
            
            
    def reducer(self, doc_id, words):        
        #we are receiving exactly what the mapper got
        #we have self.key_docs 
        #we need to build our vocabulary based on the key documents.
        
        words = list(words)
        
        if doc_id[0] == "*": #seed - these will come first.
            self.key_docs[doc_id[1:]] = words[0]
        else:
            for w in words[0]:
                k = self.key_docs.get(doc_id, "*") #see if doc is in our key docs; if not return *
                yield [w,k], 1  #[word, k], 1  where k will be the cluster or *

        
    def mapper_vocabulary(self, wk, value):
        yield wk, value
        
    def reducer_vocabulary(self, wk, value):
        
        word,k = wk
        
        #reducer_vocabulary input ['black', 2] [1];  2 could be * if no k
        
        if self.qmk_word != word:  
            #process old word
            if len(self.qmk_word) == 0: #do alphaself.qmk = {}
                for i in range(1,self.options.k+1):
                    self.qmk_word = "*alpha"
                    self.qmk = {}
                    for i in range(1,self.options.k+1):
                        self.qmk[i] = 1.0/self.options.k
            
                #output to file (append)
                outputList =  str(self.qmk_word) + "\t"+ str(self.qmk)
                #print outputList

                fullPath = self.options.pathName + 'AlphaResults.txt'
                fileOut = open(fullPath,'a')
                fileOut.write(str(outputList)+"\n")
                fileOut.close()
            else:
                #output to file (append)
                outputList =  str(self.qmk_word) + "\t"+ str(self.qmk)
                #print outputList

                fullPath = self.options.pathName + 'WordResults.txt'
                fileOut = open(fullPath,'a')
                fileOut.write(str(outputList)+"\n")
                fileOut.close()

            #initialize for new word
            self.qmk_word = word
            self.qmk = {}
            for i in range(1,self.options.k+1):
                self.qmk[i] = 0
            if k != "*": self.qmk[k]=list(value)[0]
           
        else:  #same word
            if k != "*": self.qmk[k] = list(value)[0]
            
    def reducer_vocabulary_final(self):
        #emit last word
        outputList =  str(self.qmk_word) + "\t"+ str(self.qmk)
        fullPath = self.options.pathName + 'WordResults.txt'
        fileOut = open(fullPath,'a')
        fileOut.write(str(outputList))
        fileOut.close()

if __name__ == '__main__':
    BMM_Init.run()