from __future__ import division, print_function
import numpy as np
import os,sys
from h5boss import boss2hdf5
import pandas as pd
import traceback
from mpi4py import MPI

bosspath="/global/cscratch1/sd/jialin/bossfits/"
#outputpath="/global/cscratch1/sd/jialin/h5boss_v2/"
outputpath="/global/cscratch1/sd/jialin/h5boss/"
version=1
def listfiles(datapath):
     ldir=os.listdir(datapath)
     lldir=[fn for fn in ldir if fn.isdigit()]
     return lldir

#df=listfiles(bosspath)
df = pd.read_csv("missplate1.csv",index_col=None,dtype=str,header=-1)
ldf=len(df)
def findseed(x):
     fitsfiles = [os.path.join(root, name)
       for root, dirs, files in os.walk(x)
       for name in files
        if name.startswith("spPlate") and name.endswith(".fits")]
     return fitsfiles

total_fit=0
missfits=list()
import csv
def count_allfits(df):
     global total_fit,missfits
     print("number of plate:%d"%len(df))
     for i in range(0,len(df)):
       #print("in plate folder:%s"%df[i])
       bossplate=bosspath+df[i]
       fitsf=findseed(bossplate)
       #print("found %d spPlate files"%len(fitsf))
       total_fit+=len(fitsf)
       #check each fits file, if not converted, convert it
       for j in range(0,len(fitsf)):
        hdf5file=outputpath+fitsf[j].split('/')[-1].replace('spPlate-','',1).replace('fits','hdf5',1)
        if(not os.path.isfile(hdf5file)):
          #print("now converted:%s"%hdf5file.split('/')[-1])
          #print("converting it into version %d"%version)
          #boss2hdf5.serial_convert(fitsf[j],hdf5file,str(version))
          missfits.append(str(df[i])) 
     print("found %d"%len(missfits)) 
     myset=set(missfits)
     missfitsu=list(myset)
     print("found unique %d"%len(missfitsu))
     with open("missplate1.csv", "wb") as f:
      writer = csv.writer(f)
      writer.writerows(missfitsu)
rank = MPI.COMM_WORLD.Get_rank()
nproc= MPI.COMM_WORLD.Get_size()
start=int(sys.argv[1])
if rank<nproc:
  bossplate=bosspath+df[0][start+rank]
  fitsf=findseed(bossplate)
  for i in range(0,len(fitsf)):
   try:
    hdf5file=outputpath+fitsf[i].split('/')[-1].replace('spPlate-','',1).replace('fits','hdf5',1)
    #boss2hdf5.serial_convert(fitsf,outputpath+hdf5file)
    if (not os.path.isfile(hdf5file)):
       print ("converting %s into %s"%(fitsf[i],str(bossplate)))
       boss2hdf5.serial_convert(fitsf[i],hdf5file,str(version))
       print ("converted %s ok"%hdf5file)
   except Exception as e:
    #traceback.print_exc()
    print ("error in convertion %s"%hdf5file)
    #errcheck+=1
    pass
else:
 print (rank)
