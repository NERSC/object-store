import csv
import pandas as pd
import random
import sys
input=sys.argv[1]
infile=[]
try:
  with open(input,'rb') as f:
    reader = csv.reader(f)
    infile = list(reader)
except Exception, e:
 print (" file open error: %s"%e,input)
infile = [x for sublist in infile for x in sublist]
random.shuffle(infile)
fout=open('pmf-shuffle','w')
writer=csv.writer(fout,delimiter='\n')
writer.writerow(infile)

 
