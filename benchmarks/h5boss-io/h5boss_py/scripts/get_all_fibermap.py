#!/usr/bin/env python
"""
MPI version of get_fiber map, return all dataset as k,v(plate/mjd/fiber/../dataset, (type,shape,filename_path))
"""
from __future__ import division, print_function
import cPickle as pickle
from collections import defaultdict
import h5py
import time
import os,sys
from h5boss.h5map import map_fiber
from h5boss.selectmpi_v1 import add_dic
from mpi4py import MPI 

pkfile="allfiber.pk"
dirpath="/global/cscratch1/sd/jialin/h5boss"
comm=MPI.COMM_WORLD
nproc= comm.Get_size()
rank= comm.Get_rank()
allfiles_paths = [os.path.join(dirpath,fn) for fn in next(os.walk(dirpath))[2]][0:128]
total_files=len(allfiles_paths)
print ("number of hdf5 files: %d\n"%total_files)

global_fiber_dm={}
global_time=0
step=int(total_files / nproc)
rank_start =int( rank * step)
rank_end = int(rank_start + step)
if(rank==nproc-1):
   #adjust the last rank's range
   rank_end=total_files
   if rank_start>total_files:
      rank_start=total_files
range_files=allfiles_paths[rank_start:rank_end]

for i in range(0,len(range_files)):
    tstart=MPI.Wtime()
    try:
     fiber_item = map_fiber(range_files[i])
     if len(fiber_item)>0:
       global_fiber_dm.update(fiber_item)
    except Exception as e:
     print ("error in file %s"%(allfiles_paths[i])) 
     pass 
    tend=MPI.Wtime()
    print ("Rank:%d File %d scanning %d dataset objects cost %f\n"%(rank,i,len(fiber_item),tend-tstart))
    global_time+=tend-tstart
comm.Barrier()
counterop = MPI.Op.Create(add_dic, commute=True)

#global_fiber_dms= comm.allreduce(global_fiber_dm, op=counterop)

print("Rank:%d Total cost %f\n"%(rank,global_time))
tpkstart=time.time()
try:
 pickle.dump(global_fiber_dm,open(pkfile,"wb"))
except Exception as e:
 print ("pickle error")
 pass
tpkend=time.time()
print ("Pickling cost %f\n"%(tpkend-tpkstart))
