#!/usr/bin/env python
"""
Create an HDF5 file from BOSS data

TODO:
  - include comments in meta/attrs
  - platelist quantities
"""
from __future__ import division, print_function
#from __future__ import absolute_import
from mpi4py import MPI
import h5py
from h5boss.pmf import parse_csv
from h5boss.pmf import get_fiberlink_v1
from h5boss.pmf import get_catalogtypes
from h5boss.pmf import count_unique
from h5boss.pmf import locate_fiber_in_catalog
from h5boss.selectmpi_v1 import add_dic
from h5boss.selectmpi_v1 import add_numpy
from h5boss.selectmpi_v1 import create_template
from h5boss.selectmpi_v1 import overwrite_template
from time import gmtime, strftime
from h5boss.h5map import query_datamap
import datetime
import sys,os
import os.path
import time
import optparse
import csv
import traceback
import pandas as pd
import numpy as np
import optparse
import argparse
import datetime
from collections import defaultdict
import cPickle as pickle
from copy import deepcopy
import gzip 
#@profile

def parallel_select():
    '''
    Select a set of (plates,mjds,fibers) from the realesed BOSS data in HDF5 formats.
    
    Args:
        input:   HDF5 files list, i.e., source data, [csv file]
        output:  HDF5 file, to be created or updated
        pmf: Plates/mjds/fibers numbers to be quried, [csv file]
       
    '''
    parser = argparse.ArgumentParser(prog='subset_mpi')
    parser.add_argument("input",  help="HDF5 input list")
    parser.add_argument("output", help="HDF5 output")
    parser.add_argument("pmf",    help="Plate/mjd/fiber list")
    parser.add_argument("--template", help="Create template only,yes/no/all")
    parser.add_argument("--mpi", help="using mpi yes/no")
    parser.add_argument("--fiber", help="specify fiber csv output")
    parser.add_argument("--catalog", help="specify catalog csv output")
    parser.add_argument("--datamapr", help="specify pre-scanned datamap gzip pickle file")
    parser.add_argument("--datamapw", help="specify datamap for dumpping selected map based on pmf query")
    parser.add_argument("--rwmode", help="specify readwrite mode, r or rw")
    opts=parser.parse_args()

    infiles = opts.input
    outfile = opts.output
    pmflist = opts.pmf
    fiberout = "fibercsv"
    catalogout = "catalogcsv"
    datamapr=""
    if opts.datamapr:
     datamapr=opts.datamapr
    datamapw=""
    if opts.datamapw:
     datamapw=opts.datamapw
    rwmode="rw"
    if opts.rwmode:
     rwmode=opts.rwmode
    if opts.fiber:
     fiberout = opts.fiber
    if opts.catalog:
     catalogout = opts.catalog
    catalog_meta=['plugmap', 'zbest', 'zline',
                        'match', 'matchflux', 'matchpos']
    meta=['plugmap', 'zbest', 'zline',
                        'photo/match', 'photo/matchflux', 'photo/matchpos']
    if opts.template is None or opts.template=="no":
       template=0
    elif opts.template and opts.template=="yes":
       template=1
    elif opts.template and opts.template=="all":
       template=2
    if opts.mpi is None or opts.mpi=="no": 
        print ("Try the subset.py or subset command")
        sys.exit()
    elif opts.mpi and opts.mpi=="yes":
        comm =MPI.COMM_WORLD
        nproc = comm.Get_size()
        rank = comm.Get_rank()
##______Query _________________ ##
        tstartcsv=MPI.Wtime()
        (plates,mjds,fibers,hdfsource) = parse_csv(infiles, outfile, pmflist,rank)
        tstart=MPI.Wtime()
        total_files=len(hdfsource)
        global_fiber={}
        using_pickle=0

##______Query with pre-scanned metadata, pickle file_______ ##
        if (opts.datamapr):# can use a ready pickle, or use a total pickle to search through it
            if (os.path.isfile(datamapr)):
               global_fiber=query_datamap(datamapr,plates,mjds,fibers) #abort if datamap is not complete, e.g., file not matching with infiles
               using_pickle=1

##______Query by scanning all files_________________ ##
        if(using_pickle==0):               
            ##distribute the workload evenly
            step=int(total_files / nproc)
            rank_start =int( rank * step)
            rank_end = int(rank_start + step)
            if(rank==nproc-1):
                ##adjust the last rank's range
                rank_end=total_files
                if rank_start>total_files:
                  rank_start=total_files
            range_files=hdfsource[rank_start:rank_end]
            if rank==0:
                sample_file=range_files[0]
            fiber_dict={}
            for i in range(0,len(range_files)):
                fiber_item = get_fiberlink_v1(range_files[i],plates,mjds,fibers)
                if len(fiber_item)>0:
                   fiber_dict.update(fiber_item)
        tend=MPI.Wtime()

##______Reducing Meta_______________________________## 
        if (using_pickle==0):
	    counterop = MPI.Op.Create(add_dic, commute=True) 
	    fiber_item_length=len(fiber_dict)
	    fiber_dict_tmp=deepcopy(fiber_dict)
	    global_fiber= comm.allreduce(fiber_dict_tmp, op=counterop)    
	treduce=MPI.Wtime()   

        if rank==0:
         print ("Number of processes %d"%nproc)
         print ("Length of global_fiber: %d"%(len(global_fiber)))
         #print ("parse csv time: %.2f"%(tstart-tstartcsv))
         print ("Get fiber metadata costs: %.2f"%(tend-tstart))
         print ("Allreduce dics costs: %.2f"%((treduce-tend)))

        create_file_by_all=0 # by default, only use 1 process to create the template file. 

##______Write out selected pre-scanned metadata, pickle file_______ ##
        if (opts.datamapw):
           with gzip.open(datamapw,"wb") as f3:
              pickle.dump(global_fiber,f3)

        tdump=MPI.Wtime()
        if rank==0:
         print ("Finish scanning/reducing/dumpping at %.2f"%(tdump-tstart))

##______Create File(Fiber only, for test) by all processes,sequentially____##  
        if(using_pickle==0 and rwmode=="rw" and create_file_by_all==1 and (template==1 or template==2)):
           temp_start=MPI.Wtime()
           try:
            hx = h5py.File(outfile,'a',driver='mpio', comm=MPI.COMM_WORLD,libver="latest")
            hx.flush()
            hx.close()
            comm.Barrier()
            if rank==0:
              if os.path.isfile(outfile):
                print("file created")
              else:
                print ("file error")
            comm.Barrier()
            for i in range(0,nproc):           
             if rank==i:
               create_template(outfile,fiber_dict,'fiber',rank)
             comm.Barrier()
            comm.Barrier()
           except Exception as e:
            print("template creation error")
            traceback.print_exc()
            pass
           temp_end=MPI.Wtime()
           if rank==0:
            print ("Template creation time: %.2f"%(temp_end-temp_start))
##______Create File by one process, using pickle or not_________##  
        if rank==0 and rwmode=="rw" and create_file_by_all==0 and (template==1 or template==2):
           try:
            if using_pickle==1: 
             create_template(outfile,global_fiber,'fiber',rank)
            else:
	     create_template(outfile,global_fiber,'fiber',rank)
##___________Disable catalog operation for test fiber only_________## 
#            catalog_number=count_unique(global_fiber) #(plates/mjd, num_fibers)
#            print ('number of unique fibers:%d '%len(catalog_number))           
#            catalog_types=get_catalogtypes(sample_file) # dict: meta, (type, shape)
#            global_catalog=(catalog_number,catalog_types)
#            create_template(outfile,global_catalog,'catalog',rank)
           except Exception as e:
            print("template creation error")
            traceback.print_exc()
            pass
        comm.Barrier()
        tcreated=MPI.Wtime()
        if rank==0:
           print ("Out:Template creation time: %.2f"%(tcreated-treduce))

##_______________ Fiber sorting for catalog thing________##TODO: Check later IO 
#       copy_global_catalog=global_fiber.copy() # shallow copy
#       revised_dict=locate_fiber_in_catalog(copy_global_catalog)
        #now revised_dict has: p/m, fiber_id, infile, global_offset
#       copy_revised_dict=revised_dict.items()
#       total_unique_fiber=len(copy_revised_dict)
        #distribute the workload evenly to each process
#       step=int(total_unique_fiber / nproc)+1
#       rank_start =int( rank * step)
#       rank_end = int(rank_start + step)
#       if(rank==nproc-1):
#        rank_end=total_unique_fiber # adjust the last rank's range
#        if rank_start>total_unique_fiber:
#           rank_start=total_unique_fiber
#       catalog_dict=copy_revised_dict[rank_start:rank_end]


##______Open the template collectively ___________________##
        if template ==0 or template==2: # use previously created template or new template created within this job
         try: 
          #collectively open file
          hx = h5py.File(outfile,'a',driver='mpio', comm=MPI.COMM_WORLD)
          hx.atomic=False 
         except Exception as e:
          traceback.print_exc()
          pass        
        topen=MPI.Wtime()
        tclose=topen
##______Read/Write data collectively ___________________##        
        if (template==0 or template==2) and (rwmode=="r" or rwmode=="rw"):
           fiber_copyts=MPI.Wtime()
           overwrite_template(hx,fiber_dict,'fiber',rwmode)
#          overwrite_template(outfile,fiber_dict,'fiber')
           fiber_copyte=MPI.Wtime()
           #print ("rank:%d\tlength:%d\tcost:%.2f"%(rank,fiber_item_length,fiber_copyte-fiber_copyts))
#           #for each fiber, find the catalog, then copy it
#           catalog_copyts=MPI.Wtime()
#           overwrite_template(hx,catalog_dict,'catalog')
#           catalog_copyte=MPI.Wtime()
           comm.Barrier()
           tclose_s=MPI.Wtime()
           hx.close()
           tclose=MPI.Wtime()
        if rank==0 and (template==0 or template==2) and (rwmode=="r" or rwmode=="rw"):
           print ("Fiber copy(%s): %.2f\nTotal Cost: %.2f"%(rwmode,fiber_copyte-fiber_copyts,tclose-tstart))
           #print ("File open: %.2f\nFiber copy: %.2f\nFile close: %.2f\nTotal Cost: %.2f"%(topen-tcreated,fiber_copyte-fiber_copyts,tclose-tclose_s,tclose-tstart))
        if rank==0 and template==1:
           print ("Total Cost: %.2f"%(tclose-tstart))
        if rank==0:
           try:
              print ("Total Cost: %.2f"%(tclose-tstart))
           except Exception as e:
              pass 
if __name__=='__main__': 
    parallel_select()
