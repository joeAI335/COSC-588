
#!/usr/bin/env python2

# Output the number of URLs served on each day
# input is a weblog

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os

class report1(MRJob):
    def mapper(self, _, line):
		log = Weblog(line)
		yield log.date,1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__=="__main__":
    report1.run()
