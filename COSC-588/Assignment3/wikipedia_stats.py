import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class wiki(MRJob):
    SORT_VALUES=True
    def mapper(self, _, line):
        try:
            fields = line.split("\t")
        except ValueError:
            pass
        if fields:
            try:
                month = fields[2]
            except ValueError:
                pass
        if month:
            date = month.split(" ")
            m = date[0].split("-")
            yield m[0] + "-" + m[1], 1

    def reducer(self, key, values):
        yield key, sum(values)

    def mapper2(self, key, values):
        yield "month", (key, values)

    def reducer2(self, key, values):
        for value in values:
            yield value

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            
            MRStep(mapper=self.mapper2,
                   reducer=self.reducer2)
                ]
#    def mapper(self, _, line):
#		try:
#			fields = line.split("\t")
#		except ValueError:
#			pass
#		if fields:
#			try:
#				month = fields[2]
#			except ValueError:
#				pass
#		if month:
#                date = month.split(" ")
#                m = date[0].split("-")
#            yield m[0] + "-" + m[1], 1
#
#
#    def reducer(self, key, values):
#        yield key, sum(values)
#

if __name__=="__main__":
    wiki.run()

