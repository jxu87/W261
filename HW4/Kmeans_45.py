#!/usr/bin/python
## Kmeans_45.py
## Author: Angela Gunn & Jing Xu
## Description: Does kmeans

import numpy as np
from numpy import argmin, array, random
from mrjob.job import MRJob
from mrjob.step import MRJobStep
from itertools import chain




#Calculate find the nearest centroid for data point 
def MinDist(datapoint, centroid_points):
    # calculate euclidean distance
    euclidean_distance = np.sum((datapoint - centroid_points)**2, axis = 1)
    # get the nearest centroid for each instance
    minidx = np.argmin(euclidean_distance)
    return minidx

#Check whether centroids converge
def stop_criterion(centroid_points_old, centroid_points_new,T):
    return np.alltrue(abs(np.array(centroid_points_new) - np.array(centroid_points_old)) <= T)

class MRKmeans(MRJob):
    centroid_points=[]
    CENTROID = "Centroids.txt"
    #k=4    
    def steps(self):
        return [
            MRJobStep(mapper_init = self.mapper_init, mapper=self.mapper,combiner = self.combiner,reducer=self.reducer)
               ]
    
    #load centroids info from file
    def mapper_init(self):
        self.centroid_points = [map(float,s.split('\n')[0].split(',')) for s in open(self.CENTROID).readlines()]
        open(self.CENTROID, 'w').close()
    
    #load data and output the nearest centroid index and data point 
    # returns key = nearest centroid, values = tuple(features, class:1)
    def mapper(self, _, line):
        terms = line.strip().split(',')
        userid = terms[0]
        code = int(terms[1]) #what type of user
        total = int(terms[2])
        features = np.array([float(x) / total  for x in terms[3:]]) #normalize; features = words

        # key    = centroid
        # values = tuple(features, code:1)
        yield int(MinDist(features, self.centroid_points)), (list(features), {code:1})
    
    #Combine sum of data points locally
    # returns key = idx, values = tuple(features, class:n) where n is the new count
    def combiner(self, idx, inputdata):
        combine_features = None  #features = words
        combine_codes = {}       #codes = class

        for features, code in inputdata:  #for each input line, get the features (word counts) and the class info
            features = np.array(features)
            
            # local aggregate of features
            if combine_features is None:
                combine_features = np.zeros(features.size)
            combine_features += features

            # count number of codes
            for k, v in code.iteritems():
                combine_codes[k] = combine_codes.get(k, 0) + v

        yield idx, (list(combine_features), combine_codes)
        
    #Aggregate sum for each cluster and then calculate the new centroids
    #same as reducer, but calculates new centroids as key instead of feature list.
    def reducer(self, idx, inputdata): 
        combine_features = None  #features = words
        combine_codes = {}       #codes = class
        
        for features, code in inputdata:  #for each input line, get the features (word counts) and the class info
            features = np.array(features)

            # local aggregate of features
            if combine_features is None:
                combine_features = np.zeros(features.size)
            combine_features += features

            # count number of codes
            for k, v in code.iteritems():
                combine_codes[k] = combine_codes.get(k, 0) + v

        # new centroids
        centroids = combine_features / sum(combine_codes.values())

        yield idx, (list(centroids), combine_codes)

if __name__ == '__main__':
    MRKmeans.run()