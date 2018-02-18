#!/usr/bin/env python
"""
Create an HDF5 file from BOSS data

TODO:
  - include comments in meta/attrs
  - platelist quantities
"""
from __future__ import division, print_function
#from __future__ import absolute_import
from h5boss.select import select
from h5boss.select_add import select_add
from h5boss.select_update import select_update
from h5boss.pmf import pmf
import sys,os
import time
import optparse
import csv
import traceback

parser = optparse.OptionParser(usage = "%prog [options]")
parser.add_option("-b", "--base", type=str,  help="base file")
parser.add_option("-i", "--input", type=str,  help="files to be quried")
parser.add_option("-x", "--xls", type=str,help="plate/mjd/fiber excel file")
opts, args = parser.parse_args()

xlsfile = opts.xls
infiles = opts.input
outfile = opts.base
tstart=time.time()
import pandas as pd
try: 
 df = pd.ExcelFile(xlsfile).parse('Sheet1')
 plates = map(str,df['plates'].values.tolist())
 mjds = map(str,df['mjds'].values.tolist())
 fibers = map(str,df['fibers'].values.tolist())

except Exception, e:
 print("excel read error or not exist:%s"%e,xlsfile)
 #traceback.print_exc()


try:
 with open(infiles,'rb') as f:
  reader = csv.reader(f)
  infile = list(reader)

except Exception, e:
 print ("CSV file open error or not exist: %s"%e,infiles)
 #traceback.print_exc()

infile = [x for sublist in infile for x in sublist]
#print ("Plates: ",plates)
#print ("MJDs: ",mjds)
#print ("Fibers: ", fibers)
print ("Plates/Mjds/Fibers: %d tuples"%len(plates))
#print ("Input: %d files:"%len(infile),infile[0],"...",infile[-1])
print ("Input: %d files"%len(infile))
print ("Output: ", outfile)
print ("Running selection:")

try:
 select_update(infile, outfile, plates, mjds, fibers)
#add to the pre-existing outfile
except Exception, e:
 print ("Error in select:")
 traceback.print_exc()


print ("Done selection")
