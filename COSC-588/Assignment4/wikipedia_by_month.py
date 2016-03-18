#!/usr/bin/spark-submit
#
# Problem Set #4
# Implement wordcount on the shakespeare plays as a spark program that:
# a.Removes characters that are not letters, numbers or spaces from each input line.
# b.Converts the text to lowercase.
# c.Splits the text into words.
# d.Reports the 40 most common words, with the most common first.

# Note:
# You'll have better luck debugging this with ipyspark

import sys
from operator import add
from pyspark import SparkContext
import re
if __name__ == "__main__":
    
    ##
    ## Parse the arguments
    ##

    infile =  's3://gu-anly502/ps03/freebase-wex-2009-01-12-articles.tsv'
    #infile = 'hdfs://ip-172-31-0-138.ec2.internal:8020/user/hadoop/hadoop/test.tsv'

    ## 
    ## Run WordCount on Spark
    ##

    sc     = SparkContext( appName="Wikipedia Count" )
    lines  = sc.textFile( infile )

    ## YOUR CODE GOES HERE
    ## PUT YOUR RESULTS IN counts

    # def map(line):
    #     tokens = re.split("[\\s]+", line.strip())
    #     tokens = re.split("[\\-]+", tokens[0].strip())
    #     monthToCnt = {tokens[1]:1}
    #     return monthToCnt

    # def reduce(monthToCnt1, monthToCnt2):
    #     monthToCnt = {}
    #     for month in monthToCnt1:
    #         if month not in monthToCnt:
    #             monthToCnt[month] = 0
    #         monthToCnt[month] = monthToCnt[month] + monthToCnt1[month]

    #     for month in monthToCnt2:
    #         if month not in monthToCnt:
    #             monthToCnt[month] = 0
    #         monthToCnt[month] = monthToCnt[month] + monthToCnt2[month]
    #     return monthToCnt
    # def mapper(line):
    #     line = line[0:7]
    #     return line, 1
    # def reducer(line, key):
    #     return (line, sum(key))

    # monthToCnt = lines.map(lambda line: line.split('\t')).map(lambda list : list[2]).map(map).reduce(reduce)
    counts = lines.flatMap(lambda line: line.split('\t'))\
             .filter(lambda word: word.startswith("20")) \
             .map(lambda word: (word[0:7], 1)) \
             .reduceByKey(lambda a, b: a + b) \
             .sortBy(lambda x: x[0])
    
    # for month in monthToCnt:
    #     print month + "\t" + str(monthToCnt[month]) + "\n" 
    

             # .flatMap(lambda line: line.split(' ')) \
             # .flatMap(lambda line: line.split('-')) \

    # counts = counts[0] + counts[1]

    # counts = counts.map(lambda word: (word[0:7], 1)) \
    #          .reduceByKey(lambda a, b: a + b) 

    #top40counts = counts.takeOrdered(40, key=lambda x: -x[1]) #counts.takeOrdered(40, key=lambda x: -x[1])
    counts = counts.collect()
    with open("wikipedia_by_month.txt","w") as fout:
        for (date, count) in top40counts:
            fout.write("{}\t{}\n".format(date,count))
    
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
