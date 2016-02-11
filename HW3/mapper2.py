#!/usr/bin/python
## mapper2.py
## Author: Jing Xu
## Description: mapper2 code for HW3.5

import sys
import ast

# read input from STDIN (standard input)
for line in sys.stdin:
    # split the line into words
    line = line.strip('\n')
    key, stripe = line.split('\t', 1)
    dic = ast.literal_eval(stripe)
    elements = dic.items()
    for element in elements:
        pair = '%s,%s' % (key, element[0])
        count = int(element[1])
        print '%s\t%d' % (pair, count)