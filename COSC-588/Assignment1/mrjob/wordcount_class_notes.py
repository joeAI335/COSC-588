#!/usr/bin/env python
from mrjob.job import MRJob
import heapq
TOPN = 10
class WordCount(MRJob):
	  def mapper(self, _, line):
		  for word in line.strip().split():
			  #word = filter(str.isalpha,word.lower())
			  word = word.lower()
			  yield word,1
	  def reducer_init(self):
		  print("reducer_init")
		  self.heap = []
	  def reducer(self, key, values):
		  heapq.heappush(self.heap,(sum(values),key))
		  if len(self.heap) > TOPN:
			  heapq.heappop(self.heap)
		  #print(self.heap)
	  def reducer_final(self):
		  for v in sorted(self.heap, reverse=True):
			yield v
if __name__=="__main__":
          WordCount.run()
