#! /usr/bin/python

import sys
import os

word_list = {}


inv_word_list = []
    

for line in sys.stdin:
    
    word, count = line.strip().split("\t", 1)
    
    try:
        count = int(count)
    except ValueError:
        continue
    
    if word not in word_list:
        word_list[word] = count
    else:
        word_list[word] += count



for key, value in word_list.items():
        inv_word_list.append((value, key))

inv_word_list = sorted(inv_word_list,reverse = True)

for word in inv_word_list[:10]:
    print ("%d\t%s" % (word[0], word[1]))
