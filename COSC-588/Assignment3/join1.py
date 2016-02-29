#!/usr/bin/env python2

# To get started with the join, 
# try creating a new directory in HDFS that has both the fwiki data AND the maxmind data.

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os

class FwikiMaxmindJoin(MRJob):
    def mapper(self, _, line):
        # Is this a weblog file, or a MaxMind GeoLite2 file?
        filename = mrjob.compat.jobconf_from_env("map.input.file")
        if "top1000ips_to_country.txt" in filename:
            # Handle as a GeoLite2 file
            #
            self.increment_counter("Info", "ips", 1)
            fields = line.split("\t")
            yield fields[0], ("Country", fields[1])  
        #elif "access.log" in filename:
        elif "access.log" in filename:
            # Handle as a weblog file
            self.increment_counter("Info","urls",1)
            log = Weblog(line)
            if log:
				yield log.ipaddr, ("URL",  (log.ipaddr, log.url, log.date))
        
        
        # output <date,1>
        #yield log.date, 1


    def reducer(self, key, values):
        country = None
        for v in values:
            if len(v)!=2:
                self.increment_counter("Warn","Invalid Join",1)
                continue
            if v[0]=='Country':
                country = v[1]
                continue
            if v[0]=='URL':
                ip = v[1]
                if country:
                    #assert key == country
                    #assert key == ip[0]
                    #yield ip[0],(ip[1], country[0])
                    #yield ip[0], (country, ip[1])
                    yield country, ip[1]
                else:
					#count that cannot join
                    self.increment_counter("Warn","URL without IP", 1)
                    #yield ip[0],(ip[1],"n/a")


if __name__=="__main__":
    FwikiMaxmindJoin.run()
