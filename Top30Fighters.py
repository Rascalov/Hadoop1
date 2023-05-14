from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class Top30Fighters(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                    combiner = self.combiner,
                    reducer=self.reducer),
            MRStep(reducer=self.top_30)
        ]

    def mapper(self, _, line):
        if line.startswith('R_fighter'): 
            return
        row = next(csv.reader([line]))
        year = int(row[3][:4])
        if(year > 2011 or year < 1997  ):
            return
        if (row[5] == "Red"):
            winner = row[0]
        elif (row[5] == "Blue"):
            winner = row[1]
        else:
            return
        
        yield winner, 1
        
    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield None, (sum(values), key) 

    def top_30(self, _, pairs):
        for pair in sorted(pairs, key=lambda x: x[0], reverse=True)[:30]:
            yield pair[1], pair[0]

if __name__ == '__main__':
    Top30Fighters.run()