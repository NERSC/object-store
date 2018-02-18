import unittest
from h5boss.remove import remove
import sys,os
import csv
import pandas as pd 

class Testremove(unittest.TestCase):
 def test_remove(self):
  xlsfile = 'pmf.xlsx'
  outfile = 'output.h5'
  try: 
   df = pd.ExcelFile(xlsfile).parse('Sheet1')
   plates = map(str,df['plates'].values.tolist())
   mjds = map(str,df['mjds'].values.tolist())
   fibers = map(str,df['fibers'].values.tolist())

  except Exception, e:
   print("excel read error or not exist:%s"%e,xlsfile)

  #try:
  remove(outfile, plates, mjds, fibers)

  #except Exception, e:
  # print ("Error in select:")

if __name__ == '__main__':
   unittest.main()
