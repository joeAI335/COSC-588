import sys
from operator import add
from pyspark import SparkContext
import re
import  socket 
import struct


def isokay(ch):
    return ch in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
if __name__ == "__main__":
    
    ##
    ## Parse the arguments
    ##

    prefixfile =  's3://gu-anly502/maxmind/GeoLite2-Country-Blocks-IPv4.csv'
    countryfile = 's3://gu-anly502/maxmind/GeoLite2-Country-Locations-en.csv'
    #infile = 'hdfs://ip-172-31-0-138.ec2.internal:8020/user/hadoop/hadoop/test.tsv'

    ## 
    ## Run WordCount on Spark
    ##

    sc     = SparkContext( appName="Wikipedia Count" )
    prefix  = sc.textFile( prefixfile )
    country = sc.textFile(countryfile)


    def ip2long(ip):
        try:
            packedIP = socket.inet_aton(ip)
        except ValueError:
            pass
        return struct.unpack("!L", packedIP)[0]

    def combineIP(line):
        lines = line.split(',')
        return (lines[1], lines[0])

    def combineCountry(line):
        lines = line.split(',')
        return (lines[0], lines[5])

    ## YOUR CODE GOES HERE
    ## PUT YOUR RESULTS IN counts

   
    
    
    ipinfo = prefix.flatMap(lambda line: combineIP(line))
    #ipinfo = ipinfo.map(lambda line: combineIP(line))
    #ipinfo = ipinfo.map(lambda ip: ip2long(ip))
    countryinfo = country.flatMap(lambda line: combineCountry(line)).map(lambda line: )
    # top40counts = joinResult.takeOrdered(40) 
    #result = ipinfo.join(countryinfo)
    #result = result.map(lambda word: ip2long(word))
    top40counts = result.takeOrdered(40)
    with open("wikipedia_by_month.txt","w") as fout:
        for (ip, country) in top40counts:
            fout.write("{}\t{}\n".format(ip,country))
    
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
