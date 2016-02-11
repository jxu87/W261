#!/usr/bin/python
## combiner.py
## Author: Jing Xu
## Description: combiner code for HW3.2.3

import sys

current_word = None
current_count = 0
word = None

sys.stderr.write("reporter:counter:CombinerCounter,Instance,1\n")

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
            print '%s\t%s' % (current_word, current_count)
        current_count = count
        current_word = word
    sys.stderr.write("reporter:counter:CombinerCounter,Line,1\n")

# do not forget to output the last word if needed!
if current_word == word:
    print '%s\t%s' % (current_word, current_count)
    