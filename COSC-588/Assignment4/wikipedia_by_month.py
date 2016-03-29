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
import matplotlib.pyplot as plt

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

    
    # monthToCnt = lines.map(lambda line: line.split('\t')).map(lambda list : list[2]).map(map).reduce(reduce)
    date = lines.map(lambda line: line.split('\t')[2])
    
    
    month = date.map(lambda word: (word[0:7], 1)).reduceByKey(lambda a, b: a + b) 
                
             
    sorted_monthCount = month.sortBy(lambda x: x[0]).collect()
    
    month_length = []
    i = 1
    with open("wikipedia_by_month.txt","w") as fout:
        for (date, count) in sorted_monthCount:
            fout.write("{}\t{}\n".format(date,count))
            #count how many records of months in result set
            month_length.append(i)
            #disperse the x axis 
            i += 100000
    

    #temporary variable to store counts and months
    counts = []
    months = []
    

    for (date, count) in sorted_monthCount:
        counts.append(count)
        months.append(date)

    #draw a diagram where x axis is the amount of month
    #and y axis is the counts
    plt.plot(month_length, counts)
    #replace corresponding month with x axis
    plt.xticks(month_length, months, rotation='vertical')
    #save to a pdf file
    plt.savefig("Wikipedia_by_month.pdf")
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
