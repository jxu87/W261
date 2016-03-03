from mrjob.job import MRJob

from math import sqrt, exp, pow,pi
from numpy import zeros, shape, random, array, zeros_like, dot, linalg
import json

def gauss(x, mu, P_1):
    #function for calculating gaussian
    xtemp = x - mu
    n = len(x)
    p = exp(- 0.5*dot(xtemp,dot(P_1,xtemp)))
    detP = 1/linalg.det(P_1)
    p = p/(pow(2.0*pi,n/2.0)*sqrt(detP))
    return p

class GMM_Iterate(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        #call original init function
        super(GMM_Iterate, self).__init__(*args, **kwargs)
        
        fullPath = self.options.pathName + 'intermediateResults.txt'
        
        #read input
        fileIn = open(fullPath)
        inputJson = fileIn.read()
        fileIn.close()
        inputList = json.loads(inputJson)
        
        #break out the individual lists:
        
        #prior class probabilities
        temp = inputList[0]        
        self.phi_array = array(temp)  
        #current means list
        temp = inputList[1]
        self.mean_array = array(temp)    
        #inverse covariance matrices for w, calc.
        temp = inputList[2]
        self.covariance_inverse1_array = array(temp)         
        
        
        #accumulate partial sums                               
        #sum of weights - by cluster
        self.new_phi_array = zeros_like(self.phi_array)        #partial weighted sum of weights
        self.new_means_array = zeros_like(self.mean_array)
        self.new_cov_array = zeros_like(self.covariance_inverse1_array)
        
        self.numMappers = 1             #number of mappers
        self.count = 0                  #passes through mapper
        
                                                 
    def configure_options(self):
        #call base configure_options
        super(GMM_Iterate, self).configure_options()

        self.add_passthrough_option(
            '--k', dest='k', default=3, type='int',
            help='k: number of densities in mixture')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="", type='str',
            help='pathName: pathname where intermediateResults.txt is stored')
        
    def mapper(self, key, values):
        #accumulate partial sums for each mapper
        xList = json.loads(values)
        x = array(xList)
        
        """
        Class Assignments
        Estimate class assignments(responsibilities) or weights.
        Use the current model to estimate class assignments.
        """
        #weighted vectors
        wtVect = zeros_like(self.phi_array)
        for i in range(self.options.k):
            wtVect[i] = self.phi_array[i]*gauss(x,self.mean_array[i],self.covariance_inverse1_array[i])
        wtSum = sum(wtVect)
        wtVect = wtVect/wtSum
        
        #accumulate to update est of probability densities.
        #increment count
        self.count += 1
        
    
    
        """
        M STEP
        For each cluster K, update the centroid running
        We are calculating the summation parts of the formulas
        """
        #accumulate weights for phi est  (the Prior)
        self.new_phi_array = self.new_phi_array + wtVect
    
        
        for i in range(self.options.k):
            #accumulate weighted x's for mean calc (the Centroid)
            self.new_means_array[i] = self.new_means_array[i] + wtVect[i]*x
            
            #accumulate weighted squares for cov estimate  (the Covariance)
            xmm = x - self.mean_array[i]
            covInc_array = zeros_like(self.new_cov_array[i])
            
            for l in range(len(xmm)):
                for m in range(len(xmm)):
                    covInc_array[l][m] = xmm[l]*xmm[m]
            self.new_cov_array[i] = self.new_cov_array[i] + wtVect[i]*covInc_array     
        #dummy yield - real output passes to mapper_final in self


        
    def mapper_final(self):
        #output the new arrays
        out = [self.count, (self.new_phi_array).tolist(), (self.new_means_array).tolist(), (self.new_cov_array).tolist()]
        jOut = json.dumps(out)        
        
        yield 1,jOut
    
    
    def reducer(self, key, xs):
        #accumulate partial sums
        first = True        
        
        #accumulate partial sums
        #xs us a list of paritial stats, including count, phi, mean, and covariance. 
        
        #Each stats is k-length array, storing info for k components
        for val in xs:
            if first:
                temp = json.loads(val)
                #totCount, totPhi, totMeans, and totCov are all arrays
                totCount = temp[0]
                totPhi_array = array(temp[1])
                totMeans_array = array(temp[2])
                totCov_array = array(temp[3])                
                first = False
            else:
                temp = json.loads(val)
                #cumulative sum of four arrays
                totCount = totCount + temp[0]
                totPhi_array = totPhi_array + array(temp[1])
                totMeans_array = totMeans_array + array(temp[2])
                totCov_array = totCov_array + array(temp[3])
                
                
        """
        Here, we finish the Centroid, Covariance and Prior by dividing the sums collected from the mapper
        The Prior is divided by totCount
        The Centroid and Covariance are divided by n_k (the Prior)
        """
        #finish calculation of new probability parameters. array divided by array
        #This is the PRIORS
        newPhi_array = totPhi_array/totCount
        
        #initialize these to get the right size arrays
        #Means = CENTROIDS
        #Cov = COVARIANCE
        newMeans_array = totMeans_array
        newCov_1_array = totCov_array
        for i in range(self.options.k):
            newMeans_array[i,:] = totMeans_array[i,:]/totPhi_array[i]
            tempCov_array = totCov_array[i,:,:]/totPhi_array[i]
            #almost done.  just need to invert the cov matrix.  invert here to save doing a matrix inversion
            #with every input data point.
            newCov_1_array[i,:,:] = linalg.inv(tempCov_array)
        
        outputList = [newPhi_array.tolist(), newMeans_array.tolist(), newCov_1_array.tolist()]
        jsonOut = json.dumps(outputList)
        
        #write new parameters to file
        fullPath = self.options.pathName + 'intermediateResults.txt'
        fileOut = open(fullPath,'w')
        fileOut.write(jsonOut)
        fileOut.close()

if __name__ == '__main__':
    GMM_Iterate.run()