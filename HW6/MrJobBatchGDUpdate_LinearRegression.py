from mrjob.job import MRJob
from mrjob.step import MRStep

class MrJobBatchGDUpdate_LinearRegression(MRJob):
    
    def initializeWeights(self):
        # run before the mapper processes any input
        # Read in weights
        with open('weights.txt','r') as f:
            self.weights = [float(x) for x in f.readline().split(',')]
        
        # Initialize gradient for this iteration
        self.partialGradient = [0]*len(self.weights)
        self.partialCount = 0
    
    def calculatePartialGradient(self, _, line):
        # multiply by weight in WOLS
        # keep partial sums in memory
        
        # D is one observation of our data
        # The observations are in the form (y, x, weight)
        D = (map(float, line.split(',')))
        y = D[0]
        x = D[1]
        weight = D[2]
        
        # yHat is the predicted value given current weights
        yHat = self.weights[0] + self.weights[1] * x
        
        # Update partial gradient with gradient from D
        self.partialGradient = [self.partialGradient[0] + (y - yHat) * weight,
                                self.partialGradient[1] + (y - yHat) * x * weight]
        self.partialCount += 1

    def emitPartialGradient(self):
        yield None, (self.partialGradient, self.partialCount)

    def gradient_accumulater(self, _, partialGradientRecords):
        # Accumulate partial gradient from mapper and emit total gradient 
        # Output: key = None, Value = gradient vector
        # Initialize totals
        totalGradient = [0]*2
        totalCount = 0

        # Accumulate
        for partialGradient, partialCount in partialGradientRecords:
            totalCount += partialCount
            for i in range(len(totalGradient)):
                totalGradient[i] += partialGradient[i]
        
        # Emit total gradient
        yield None, [x / totalCount for x in totalGradient]
        

    def steps(self):
        return [
                MRStep(mapper_init=self.initializeWeights,
                       mapper=self.calculatePartialGradient,
                       mapper_final=self.emitPartialGradient,
                       reducer=self.gradient_accumulater)
            ]
    
if __name__ == '__main__':
    MrJobBatchGDUpdate_LinearRegression.run()