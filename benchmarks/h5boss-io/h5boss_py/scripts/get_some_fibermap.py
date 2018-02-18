#!/usr/bin/env python
"""
Serial version of get_fiber map, return all dataset as k,v(plate/mjd/fiber/../dataset, (type,shape,filename_path))
"""
from __future__ import division, print_function
import cPickle as pickle
from collections import defaultdict
import h5py
import time
import os,sys

import sys
if len(sys.argv)==3:
  print("start/end of files?, br?")
  exit()
sfile=0
nfile=1
br="b"
try:
 nfile=int(sys.argv[2])
 sfile=int(sys.argv[1])
 br=str(sys.argv[3])
except Exception as e:
 print ("parsing 1 file by default")
 pass
if nfile <=0:
 nfile=1
if sfile<0:
 sfile=0
global_time=0
dirpath="/global/cscratch1/sd/jialin/h5boss"
allfiles_paths = [os.path.join(dirpath,fn) for fn in next(os.walk(dirpath))[2]][sfile:nfile]
total_files=len(allfiles_paths)
assert(total_files==nfile-sfile)
print ("number of hdf5 files: %d\n"%total_files)

def print_fiber(allfiles_paths):
 for i in range(0,total_files):
    tstart=time.time()
    try:
     ifile = h5py.File(allfiles_paths[i],'r')
     p=ifile.keys()[0]
     m=ifile[p].keys()[0]
     initp=ifile[p+'/'+m+'/1/coadd'].dtype
     inisp=ifile[p+'/'+m+'/1/coadd'].shape
     mismatch=0
     for j in range(1,1001):
       pmf=p+'/'+m+'/'+str(j)+'/coadd'
       idt=ifile[pmf].dtype
       ids=ifile[pmf].shape
       if initp!=idt or inisp!=ids:
          mismatch+=1
     if(mismatch>0):
       print ("%d found mismatching\n"%mismatch)
     else:
       print ("dtype:",initp,"\n\ndshape:",inisp)
    except Exception as e:
     print (i)
     print ("error in %d  file %s"%(i,allfiles_paths[i])) 
     print (e)
     pass 
    tend=time.time()

    global_time+=tend-tstart
 print("Total cost %f\n"%(global_time))


def print_exposure(allfiles_paths):
 for i in range(0,total_files):
    tstart=time.time()
    try:
     ifile = h5py.File(allfiles_paths[i],'r')
     p=ifile.keys()[0]
     m=ifile[p].keys()[0]
     #mismatch=0
     for j in range(1,1001):
       mismatch=0
       pmf=p+'/'+m+'/'+str(j)+'/exposures/'
       iexp=ifile[pmf].keys()[0]
       initp=ifile[pmf+iexp+'/'+br].dtype
       inisp=ifile[pmf+iexp+'/'+br].shape
       for eip in ifile[pmf].keys():
        idt=ifile[pmf+str(eip)+'/'+br].dtype
        ids=ifile[pmf+str(eip)+'/'+br].shape
        if initp!=idt or inisp!=ids:
           mismatch+=1
       if(mismatch>0):
        print ("%d found mismatching\n"%mismatch)
       else:
        print ("dtype:",initp,"\n\ndshape:",inisp)
    except Exception as e:
     print (i)
     print ("error in %d  file %s"%(i,allfiles_paths[i]))
     print (e)
     pass
    tend=time.time()
    global global_time
    global_time+=tend-tstart
 print("Total cost %f\n"%(global_time))

if __name__=='__main__':

   print_exposure(allfiles_paths)
