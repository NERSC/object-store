#!/usr/bin/env python
"""
Clean page cache
"""
from __future__ import division, print_function
import sys,os
import time
import optparse
import csv
import traceback
import argparse
import commands
parser = argparse.ArgumentParser(prog='drop_page')
parser.add_argument("input",  help="HDF5 input list")
opts=parser.parse_args()
infiles = opts.input
print ("input file is contained in :%s"%infiles)
infile=[]
try:
 with open(infiles,'rt') as f:
  reader = csv.reader(f)
  infile = list(reader)
  infile = [x for sublist in infile for x in sublist]
except Exception as e:
 print ("input filelist csv read error or not exist: %s"%e,infiles)
print ("number of input files:%d"%len(infile))
for i in range(0,len(infile)):
 try:
  cmdstr="./drop_file_from_page_cache %s 2>/dev/null"%(infile[i])
  (istatus, ioutput)=commands.getstatusoutput(cmdstr)
  if istatus!=0:
   print ("error in cmdstr:%s"%ioutput)
   print (cmdstr)
 except Exception as e:
  pass

