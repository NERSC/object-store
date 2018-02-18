"""
Create an HDF5 file from BOSS data

TODO:
  - include comments in meta/attrs
  - platelist quantities
"""
from __future__ import division, print_function
import unittest
from h5boss.select import select
import sys,os
import time
import optparse
import csv
import traceback
import pandas as pd 
class Testsubset(unittest.TestCase):
 def test_subset(self):
  xlsfile = 'h5boss_test_10.xlsx'
  infiles = 'input-full'
  outfile = 'output.h5'
  tstart=time.time()
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
 
  infile = [x for sublist in infile for x in sublist]
  try:
   select(infile, outfile, plates, mjds, fibers)

  except Exception, e:
   print ("Error in select:")
   #traceback.print_exc()
  print ("Done selection")

if __name__ == '__main__':
   unittest.main()
