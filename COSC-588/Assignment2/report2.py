#!/usr/bin/env python2

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os


class report2(MRJob):
    def mapper(self, _, line):
		log = Weblog(line)
		if "Special" not in log.url:
			yield log.date, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__=="__main__":
    report2.run()
