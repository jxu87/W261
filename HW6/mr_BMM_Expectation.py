from mrjob.job import MRJob
from mrjob.step import MRStep

from numpy import mat, zeros, shape, random, array, zeros_like, dot, linalg
from random import sample
import json

import ast

from math import pi, sqrt, exp, pow


class BMM_Expectation(MRJob):
    DEFAULT_PROTOCOL = 'json'
    


    
    def __init__(self, *args, **kwargs):
        #call base init method
        super(BMM_Expectation, self).__init__(*args, **kwargs)
        
        fullPath = self.options.pathName + 'WordResults.txt'
        
        self.alpha = {}
        self.q_mk = {}
        
        self.current_doc=""
        self.current_in = {}
        self.current_out = {}
        
        #build word dictionary.  Not scalable, but will do.
        with open(fullPath, "r") as f:
            for line in f:
                line = line.replace("]","").replace("[","")
                line = line.strip()
                line = line.replace("'","")
                
                
                word, dic = line.split("\t")
                
                word = word.replace("'","").strip()
                dic = ast.literal_eval(dic)
                self.q_mk[word] = dic
                
        #build alpha        
        fullPath = self.options.pathName + "AlphaResults.txt" 
    
        with open(fullPath, "r") as f:
            for line in f:
                
                line = line.replace("]","").replace("[","")
                line = line.strip()
                line = line.replace("'","")
                
                
                word, dic = line.split("\t")
                
                word = word.replace("'","").strip()
                dic = ast.literal_eval(dic)
                for k,v in dic.iteritems():
                    self.alpha[k]=v
                
        #delete contents of file doc_Results
        fullPath = self.options.pathName + 'doc_results.txt'
        with open(fullPath, "w") as f:
            pass
                
            
                                                 
    def configure_options(self):
        #call base configure_options method
        super(BMM_Expectation, self).configure_options()
        
        self.add_passthrough_option(
            '--k', dest='k', default=2, type='int',
            help='k: number of densities in mixture')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="", type='str',
            help='pathName: pathname where WordResults.txt is stored')
        
        
    def mapper(self, _, values):
        values = values.split("\t")
        doc = values[0].strip()
        words = values[1].split()
        
        k_tin = {}
        k_tout = {}
        
        
        #for each word in our dic, and then for each k, output a value (O(kw)  where w is |words|)
        for w,qmk in self.q_mk.iteritems():
            if doc in ['10']: print "w, qmk = ",w,qmk
            if sum(qmk.values()) > 0:  #we only want words where we have information

                for k in range(1, self.options.k+1):
                    if doc in ['10']: print "qmk[k]=",k, qmk[k]

                    if w in words:
                        if doc in['10']: print w, [doc, k, 1], float(qmk[k])
                        yield [doc, k, 1], float(qmk[k])
                    else:
                        if doc in['10']: print w, [doc, k, 1], 1-float(qmk[k])
                        yield [doc, k, 0], 1-float(qmk[k])
                    
                    
    
    def reducer(self, key, values):
        value = sum(values)
        doc, k, in_out = key
        
        
        fullPath = self.options.pathName + 'doc_results.txt'
        
        if self.current_doc == doc:
            if in_out == 1:
                self.current_in[k] = value
            else:
                self.current_out[k] = value
        else: #new document
            if doc in['10']: print "reducer output"
            if len(self.current_doc) > 0: #change doc
                #output
                f_k = {}
                sum_all = 0
                for i_k in range(1, self.options.k+1):
                    #set return to -1 if not found.
                    k_in = self.current_in.get(i_k,-1)
                    k_out = self.current_out.get(i_k,-1)
                    if k_in == -1 or k_out == -1:  #if we didn't find something, use alpha
                        if self.current_doc in['10']: print "shouldn't be here"
                        f_k[i_k] = self.alpha[i_k]
                    else:
                        f_k[i_k] = self.alpha[i_k] * self.current_in[i_k] * self.current_out[i_k]
                    sum_all += f_k[i_k] # sum will be used in denominator
                    if self.current_doc in['10']: print "k_in = ", self.current_in[i_k]
                    if self.current_doc in['10']: print "k_out = ",self.current_out[i_k]
                    if self.current_doc in['10']: print "alpha = ", self.alpha[i_k]
                    if self.current_doc in['10']: print "f[k] = ", f_k[i_k]
                    
                #print output
                with open(fullPath, "a") as f:
                    for i_k in range(1,self.options.k+1):
                        out_key = [self.current_doc,i_k]
                        if sum_all == 0:
                            out_val = self.alpha[i_k]
                        else:
                            out_val = f_k[i_k]/sum_all
                        
                        out_string = str(out_key) + "\t" + str(out_val)
                        if self.current_doc in['10']: print "output = ", out_string
                        #print out_string
                        f.write(out_string + "\n")
                
            #reset values
            self.current_doc = doc
            self.current_in = {}
            self.current_out = {}
            if in_out == 1:
                self.current_in[k] = value
            else:
                self.current_out[k] = value
        #end if
        
        
    def reducer_final(self):
        #final output
        fullPath = self.options.pathName + 'doc_results.txt'
        
        f_k = {}
        sum_all = 0
        for i_k in range(1, self.options.k+1):
            f_k[i_k] = self.alpha[i_k] * self.current_in[i_k] * self.current_out[i_k]
            sum_all += f_k[i_k]
        with open(fullPath, "a") as f:
            for i_k in range(1,self.options.k+1):
                out_key = [self.current_doc,i_k]
                out_val = f_k[i_k]
                out_string = str(out_key) + "\t" + str(out_val)
                f.write(out_string +"\n")
  
    
if __name__ == '__main__':
    BMM_Expectation.run()
    