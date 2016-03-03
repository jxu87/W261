from mrjob.job import MRJob
from mrjob.step import MRStep

from numpy import mat, zeros, shape, random, array, zeros_like, dot, linalg
from random import sample
import json
from math import pi, sqrt, exp, pow


class GMM_Init(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def steps(self):
        return [
                MRStep(mapper=self.mapper,
                       reducer=self.reducer)
            ]

    
    def __init__(self, *args, **kwargs):
        #call base init method
        super(GMM_Init, self).__init__(*args, **kwargs)
        
        self.numMappers = 1     #number of mappers
        self.count = 0
        
                                                 
    def configure_options(self):
        #call base configure_options method
        super(GMM_Init, self).configure_options()
        
        self.add_passthrough_option(
            '--k', dest='k', default=3, type='int',
            help='k: number of densities in mixture')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="", type='str',
            help='pathName: pathname where intermediateResults.txt is stored')
        
    def mapper(self, key, values):
        #something simple to grab random starting point
        #collect the first 2k  points (this isn't 2000, this is 2*the number of densities)
        #The values are points
        if self.count <= 2*self.options.k:
            self.count += 1
            yield (1,values)        
        
    def reducer(self, key, values):        
        #receives an integer as the key, and a data point as the values.
        
        #accumulate data points mapped to 0 from 1st mapper and pull out k of them as starting point
        centroids = []
        
        #load the values and append to centroids; yield 1, and the value
        for xj in values:
            x = json.loads(xj)
            centroids.append(x)
            yield 1, xj
            
        #sample the points - these are our new centroids
        index = sample(range(len(centroids)), self.options.k)
        centroids_sample = []        
        for i in index:
            centroids_sample.append(centroids[i])
        
        #use the covariance of the selected centers as the starting guess for covariances
        
        #first, calculate mean of centers
        #create an array of the centroids, then divide each by the number of clusters(k)
        mean_array = array(centroids_sample[0])
        for i in range(1,self.options.k):
            mean_array = mean_array + array(centroids_sample[i])
        
        
        mean_array = mean_array/float(self.options.k)
        
        
        #then accumulate the deviations
        covariance = zeros((len(mean_array),len(mean_array)),dtype=float)
        for x in centroids_sample:
            deviation = array(x) - mean_array
            for i in range(len(mean_array)):
                covariance[i,i] = covariance[i,i] + deviation[i]*deviation[i]
                
        covariance = covariance/(float(self.options.k))
        covariance_inverse = linalg.inv(covariance)
        
        covariance_inverse1 = [covariance_inverse.tolist()]*self.options.k
        
        #for debugging
        jDebug = json.dumps([centroids_sample,mean_array.tolist(),covariance.tolist(),covariance_inverse.tolist(),covariance_inverse1])    
        debugPath = self.options.pathName + 'debug_init.txt'
        fileOut = open(debugPath,'w')
        fileOut.write(jDebug)
        fileOut.close()
        
        #also need a starting guess at the phi's - prior probabilities
        #initialize them all with the same number - 1/k - equally probably for each cluster
        
        phi = zeros(self.options.k,dtype=float)
        
        for i in range(self.options.k):
            phi[i] = 1.0/float(self.options.k)
        
        #form output object
        outputList = [phi.tolist(), centroids_sample, covariance_inverse1]
                   
            
        jsonOut  = json.dumps(outputList)
        
        
        # Write to file
        fullPath = self.options.pathName + 'intermediateResults.txt'
        with open(fullPath, 'w') as outfile:
            outfile.write(jsonOut)
            

if __name__ == '__main__':
    GMM_Init.run()