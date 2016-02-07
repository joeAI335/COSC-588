#!/usr/bin/env python2

import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
from weblog import Weblog       # imports class defined in weblog.py
import os
import heapq
TOPN = 10

class report4(MRJob):
    def mapper(self, _, line):
        log = Weblog(line)
        yield log.wikipage(), 1
        
    def reducer(self, word, counts):
        yield word, sum(counts)

    def topN_mapper(self,word,count):
        yield "Top"+str(TOPN), (count,word)

    def topN_reducer(self,_,countsAndWords):
        for countAndWord in heapq.nlargest(TOPN,countsAndWords):
            yield _,countAndWord
         
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),

            MRStep(mapper=self.topN_mapper,
                   reducer=self.topN_reducer) ]


if __name__=="__main__":
    report4.run()
