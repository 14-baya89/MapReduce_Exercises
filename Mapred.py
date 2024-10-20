#Importer les bibliothèques 
import re
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

# definir la classe
class WordCounter(MRJob):

 #Definir la methode mapper 
    
    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1
 #Definir la methode combiner  
           
    def combiner(self, key, values):
        yield key, sum(values)

#Definir la methode reducer  
        
    def reducer(self, key, values):
        yield key, sum(values)

#Definir la methode steps
          
    def steps(self):
        return [
            MRStep (
                mapper=self.mapper,
                reducer= self.reducer
            )
        ]    
            
if __name__ == '__main__':
    WordCounter.run()

