from mrjob.job import MRJob
from mrjob.step import MRStep

from numpy import mat, zeros, shape, random, array, zeros_like, dot, linalg
from random import sample
import json

import ast

from math import pi, sqrt, exp, pow


class BMM_Maximization_qmk(MRJob):
    DEFAULT_PROTOCOL = 'json'
    


    
    def __init__(self, *args, **kwargs):
        #call base init method
        super(BMM_Maximization_qmk, self).__init__(*args, **kwargs)
        
        fullPath = self.options.pathName + 'WordResults.txt'
        
        self.alpha = {}
        self.q_mk = {}
        self.smoothing = 0.0
        
        self.current_word=""
        self.current_kvals = {}
        
        #delete contents of file WordResults
        with open(fullPath, "w") as f:
            pass
                
        fullPath = self.options.pathName + 'doc_results.txt'
        
        #build doc dictionary.  Not scalable, but will do.       
        self.rdk = {}  # this will look like rdk[dock][k] = value
        self.rdk_sum = {}  #hold sum of all rdk
        for k in range(1, self.options.k+1): self.rdk_sum[k] = 0
        
        with open(fullPath, "r") as f:
            for line in f:
                #cleanup
                line = line.replace("]","").replace("[","")
                line = line.strip()
                line = line.replace("'","")
                tokens, value = line.split("\t")
                doc, k = tokens.split(",")
                doc=doc.strip()
                k=int(k.strip())
                
                k_dic = self.rdk.get(doc, {})  #get the dictionary associated with this doc
                k_dic[k] = float(value)   #add the k value
                
                self.rdk[doc] = k_dic   #put the dictionary back.
                
                self.rdk_sum[k] += float(value)+ self.smoothing


            
                                                 
    def configure_options(self):
        #call base configure_options method
        super(BMM_Maximization_qmk, self).configure_options()
        
        self.add_passthrough_option(
            '--k', dest='k', default=2, type='int',
            help='k: number of densities in mixture')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="", type='str',
            help='pathName: pathname where WordResults.txt is stored')
        
        
    def mapper(self, _, values):
        #input is the documents
        #emits [word, k], rnk + smoothing
        
        doc, words = values.split("\t")
        doc = doc.strip()
        words = words.split()
        
        
        for k in range (1,self.options.k+1):
            rnk = self.rdk[str(doc)][k]
            for w in words:
                yield [w,k], rnk + self.smoothing
                    
                    
    
    def reducer(self, key, values):
        #receives [word,k], rnks
        rnk_sum = sum(values)
        word, k = key
        
        
        fullPath = self.options.pathName + 'WordResults.txt'
        
        if self.current_word != word: #new word!
            if len(self.current_word) > 0: # do something with old word
                strOutput = self.current_word + "\t" + str(self.current_kvals)
                with open(fullPath, "a") as f:
                    f.write(strOutput + "\n")
            #reset values
            self.current_word = word
            self.current_kvals = {}
            self.current_kvals[k] = float(rnk_sum)/self.rdk_sum[k]
        else:
            self.current_kvals[k] = float(rnk_sum)/self.rdk_sum[k]
        

        
        
    def reducer_final(self):
        #final output
        
        fullPath = self.options.pathName + 'WordResults.txt'
        
        if len(self.current_word) > 0: # do something with old word
            strOutput = self.current_word + "\t" + str(self.current_kvals)
            with open(fullPath, "a") as f:
                f.write(strOutput + "\n")
  
    
if __name__ == '__main__':
    BMM_Maximization_qmk.run()
    