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

if __name__ == "__main__":
    
    ##
    ## Parse the arguments
    ##

    infile =  's3://gu-anly502/ps03/freebase-wex-2009-01-12-articles.tsv'

    ## 
    ## Run WordCount on Spark
    ##

    sc     = SparkContext( appName="Wikipedia Count" )
    lines  = sc.textFile( infile )

    ## YOUR CODE GOES HERE
    ## PUT YOUR RESULTS IN counts

    counts = lines.flatMap(lambda line: line.split('\t')) \
             .flatMap(lambda line: line.split(' ')) \
             .flatMap(lambda line: line.split('-')) \

    counts = counts[0] + counts[1]

    counts = counts.map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b) 

    
    with open("wikipedia_by_month.txt","w") as fout:
        for (date, count) in counts:
            fout.write("{}\t{}\n".format(date,count))
    
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
