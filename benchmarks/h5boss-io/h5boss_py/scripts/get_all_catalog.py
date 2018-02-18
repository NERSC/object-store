import h5py 
import csv
import numpy as np
import os
import sys
import traceback
infiles='input-full-cori'
fx=h5py.File('/global/cscratch1/sd/jialin/boss_catalog.h5','w')
try:
 with open(infiles,'rt') as f:
  reader = csv.reader(f)
  infile = list(reader)
  infile = [x for sublist in infile for x in sublist]
except Exception as e:
 print ("input filelist csv read error or not exist: %s"%e,infiles)
print (len(infile))
meta=['plugmap', 'zbest', 'zline']
photo=['match','matchflux','matchpos']
for ifile in infile:
  try:
   hx=h5py.File(ifile,'r')
   plate=hx.keys()[0]
   mjd=hx[plate].keys()[0]
   parent_id='{}/{}'.format(plate,mjd)
   subparent_id='{}/{}/{}'.format(plate,mjd,'photo')
   fx.create_group(parent_id)
   fx.create_group(subparent_id)
  except Exception as e:
   print ('open file error')
   traceback.print_exc() 
   pass
  try:
   for imeta in meta:
    im='{}/{}/{}'.format(plate,mjd,imeta)
    hx.copy(im,fx[parent_id])  
  except Exception as e:
    print ("error in %s"%im)
    traceback.print_exc()
    pass
  try:
   for imeta in photo:
    im='{}/{}/{}/{}'.format(plate,mjd,'photo',imeta)
    hx.copy(im,fx[subparent_id])
  except Exception as e:
    print('error of %s'%im)
    traceback.print_exc()
    pass
  try:
    hx.close()
  except Exception as e:
    print('close file %s error'%ifile)
    traceback.print_exc()
    pass
try:
 fx.close()
except Exception as e:
 print ('error in close output')
