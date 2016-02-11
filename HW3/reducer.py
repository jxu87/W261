#!/usr/bin/python
## reducer.py
## Author: Jing Xu
## Description: reducer code for HW3.2.2

import sys

current_word = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            print '%s\t%d' % (current_word, current_count)
        current_count = count
        current_word = word
    sys.stderr.write("reporter:counter:ReducerCounter,Lines,1\n")
        
# do not forget to output the last word if needed!
if current_word == word:
    print '%s\t%d' % (current_word, current_count)

sys.stderr.write("reporter:counter:ReducerCounter,Instances,1\n")