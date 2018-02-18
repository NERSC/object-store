#!/usr/bin/env python
"""
MPI version of get_fiber map, return all dataset as k,v(plate/mjd/fiber/../dataset, (type,shape,filename_path))
"""
from __future__ import division, print_function
import cPickle as pickle
import hickle as hkl
import gzip
from collections import defaultdict
import h5py
import time
import os,sys
from h5boss.h5map import map_fiber_simple
from h5boss.selectmpi_v1 import add_dic
from mpi4py import MPI 
import json
import traceback
pkfile="allfiber.pk"
hkfile="allfiber.hk"
pkfilesimple="allfiber.pksim"
pkfilesimpleall=sys.argv[2]
hkfilesimple="allfiber.hksim"
jsfilesimple="allfiber.jssim"
dirpath="/global/cscratch1/sd/jialin/h5boss"
mapdir="/global/cscratch1/sd/jialin/h5boss/map/allmap"
comm=MPI.COMM_WORLD
nproc= comm.Get_size()
rank= comm.Get_rank()
allfiles_paths = [os.path.join(dirpath,fn) for fn in next(os.walk(dirpath))[2]][int(sys.argv[1]):int(sys.argv[1])+nproc]
total_files=len(allfiles_paths)
if rank==0:
 print ("number of hdf5 files: %d\n"%total_files)
 print ("number of processes: %d\n"%nproc)
 assert(nproc==total_files)
fiber_item={}
fiber_itemt={}
global_time=0
ifile=allfiles_paths[rank]

tstart=MPI.Wtime()
try:
  fiber_item,fiber_itemt = map_fiber_simple(ifile)
  fiber_item.update(fiber_itemt)
except Exception as e:
  print ("error in file %s"%(ifile)) 
  pass 
tend=MPI.Wtime()
    
#print ("Rank: %d, File:%s, Scanning: %d, Dataset_objects_cost: %f\n"%(rank,ifile,len(fiber_item),tend-tstart))
comm.Barrier()
#print ("Rank: %d, Pickling_cost: %f\n"%(rank,tpkend-tpkstart))

comm.Barrier()
counterop = MPI.Op.Create(add_dic, commute=True)
treduce=MPI.Wtime()
global_fiber_dms= comm.allreduce(fiber_item, op=counterop)
treduceend=MPI.Wtime()-treduce
if rank==0:
 print("Total cost:%f"%(treduceend))
#tpkstart=time.time()
#try:
# hkl.dump(fiber_item,mapdir+hkfilesimple+"_"+str(rank),mode="w")
#except Exception as e:
# print ("pickle error")
# pass
#tpkend=time.time()
#print ("Rank: %d, Hickling_cost: %f\n"%(rank,tpkend-tpkstart))
comm.Barrier()
tpkstart=time.time()
try:
 with gzip.open(mapdir+pkfilesimpleall,"wb") as f:
  pickle.dump(global_fiber_dms,f)
 #pickle.dump(fiber_item,open(mapdir+pkfilesimple+"_"+str(rank),"wb"))
 #hkl.dump(fiber_item,mapdir+hkfilesimple+"_"+str(rank),mode="w")
 #json.dump(fiber_item,open(mapdir+jsfilesimple+"_"+str(rank),mode="w"))
 #pickle.dump(fiber_itemt,open(mapdir+pkfilesimple+"t_"+str(rank),"wb"))
except Exception as e:
 traceback.print_exc()
 print ("pickle error")
 pass
tpkend=time.time()
if rank==0:
  print("Total pk cost:%f"%(tpkend-tpkstart))

