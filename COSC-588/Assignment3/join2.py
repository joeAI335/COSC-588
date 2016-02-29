#!/usr/bin/env python2

# To get started with the join, 
# try creating a new directory in HDFS that has both the fwiki data AND the maxmind data.

import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
from weblog import Weblog       # imports class defined in weblog.py
import os

#!/usr/bin/env python2

# To get started with the join, 
# try creating a new directory in HDFS that has both the fwiki data AND the maxmind data.

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os

class FwikiMaxmindJoin2(MRJob):
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
                    yield ip[0], (country, ip[1])
                else:
                    self.increment_counter("Warn","URL without IP")
                    #yield ip[0],(ip[1],"n/a")
    
    def mapper2(self, _, count):
		log = count[0]
		yield log,1

    def reducer2(self, key, values):
        yield key, sum(values)
    
    def steps(self):
		return [
			MRStep(mapper=self.mapper, 
				reducer=self.reducer),
			MRStep(mapper=self.mapper2,
				reducer=self.reducer2)
		]

if __name__=="__main__":
    FwikiMaxmindJoin2.run()
