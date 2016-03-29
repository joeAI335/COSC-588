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

def isokay(ch):
    return ch in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 '


def clean(line):
    return "".join(filter(isokay, line))

if __name__ == "__main__":
    
    ##
    ## Parse the arguments
    ##

    infile =  's3://gu-anly502/ps04/Shakespeare.txt'

    ## 
    ## Run WordCount on Spark
    ##

    sc     = SparkContext( appName="Shakespeare Count" )
    lines  = sc.textFile( infile )

    ## YOUR CODE GOES HERE
    ## PUT YOUR RESULTS IN top40counts

    # counts = lines.flatMap(lambda line: line.split()) \
    #          .map(lambda word: (word, 1)) \
    #          .reduceByKey(lambda a, b: a + b)

    #Remove characters that are not numbers, digits or spaces
    lines = lines.map(lambda line: clean(line))

    counts = lines.flatMap(lambda line: line.lower().split()) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b) \
             .sortByKey(True)
   

    top40counts = counts.takeOrdered(40, key=lambda x: -x[1])

    with open("wordcount_shakespeare4.txt","w") as fout:
        for (word, count) in top40counts:
        # for word in lines:
            fout.write("{}\t{}\n".format(word,count))
            
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
