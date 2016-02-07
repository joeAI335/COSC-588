#!/usr/bin/env python2

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os


class report3(MRJob):
    def mapper(self, _, line):
		log = Weblog(line)
		yield log.wikipage(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__=="__main__":
    report3.run()
