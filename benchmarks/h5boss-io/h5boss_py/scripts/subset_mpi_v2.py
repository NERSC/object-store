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
from h5boss.pmf import get_fiberlink_v2
from h5boss.pmf import get_catalogtypes
from h5boss.pmf import count_unique
from h5boss.pmf import locate_fiber_in_catalog
from h5boss.pmf import dedup
from h5boss.selectmpi_v2 import add_dic
from h5boss.selectmpi_v2 import add_numpy
from h5boss.selectmpi_v2 import create_template
from h5boss.selectmpi_v2 import overwrite_template
from h5boss.h5map import type_map
from h5boss.h5map import coadd_map
from time import gmtime, strftime
import datetime
import sys,os
import time
import optparse
import csv
import traceback
#import pandas as pd
import numpy as np
import optparse
import argparse
import datetime
import cPickle as pickle
from collections import defaultdict
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
    parser.add_argument("--datamap", help="specify datamap pickle file")

    opts=parser.parse_args()

    infiles = opts.input
    outfile = opts.output
    pmflist = opts.pmf
    pkfile = "datamappk"
    fiberout = "fibercsv"
    catalogout = "catalogcsv"
    pk_exist=0
    if opts.fiber:
     fiberout = opts.fiber
    if opts.catalog:
     catalogout = opts.catalog
    if opts.datamap:
     pkfile = opts.datamap
     pk_exist=1
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
##_________________ Query _________________ ## 
        tstartcsv=MPI.Wtime()
        (plates,mjds,fibers,hdfsource) = parse_csv(infiles, outfile, pmflist,rank)
        tstart=MPI.Wtime()
        total_files=len(hdfsource)
        if total_files< nproc:
           nproc=total_files
           step=1
        #distribute the workload evenly
        else:
           step=int(total_files / nproc)+1
        rank_start =int( rank * step)
        rank_end = int(rank_start + step)
        if(rank==nproc-1):
            #adjust the last rank's range
            rank_end=total_files 
            if rank_start>total_files:
             rank_start=total_files
        range_files=hdfsource[rank_start:rank_end]
        if rank==0:
          sample_file=range_files[0]
        fiber_dict={}
        for i in range(0,len(range_files)):
            fiber_item = get_fiberlink_v2(range_files[i],plates,mjds,fibers)
            if len(fiber_item)>0:
             inter_keys=fiber_dict.viewkeys() & fiber_item.viewkeys()
             if len(inter_keys)==0: 
                fiber_dict.update(fiber_item)
             else: 
                fiber_dict=fiber_union(fiber_dict,fiber_item,inter_keys)
        tend=MPI.Wtime()
        #rank0 create all, then close an reopen.-Quincey Koziol 
        #define reduce operation
        counterop = MPI.Op.Create(add_dic, commute=True) 
        global_fiber={}
        fiber_item_length=len(fiber_dict)
        fiber_dict_tmp=fiber_dict
        global_fiber= comm.allreduce(fiber_dict_tmp, op=counterop)       
        treduce=MPI.Wtime()
        if rank==0:
         print ("Number of processes %d"%nproc)
         print ("Parse csv costs: %.2f"%(tstart-tstartcsv))
         print ("Get fiber metadata costs %.2f"%(tend-tstart))
         print ("Allreduce dics costs %.2f"%((treduce-tend)))
         print ("Length of global_fiber: %d"%(len(global_fiber)))
        ## Get datamap,i.e., type and shape of each dataset in coadds and exposures. 
        if rank==0:
         tdmap_start=MPI.Wtime()
         if (pk_exist==0):
          coaddmap1=coadd_map(hdfsource)
          typemap1=type_map(sample_file)
          expb_size=4112
          expr_size=4128
          datamap1=(typemap1,coaddmap1,expb_size,expr_size)
          pickle.dump(datamap1,open(pkfile,"wb"))
         else: 
          try:
           datamap1=pickle.load(open(pkfile,"rb"))
          except Exception as e:
           print ("loading datamap pickle file:%s error"%pkfile)
           sys.exit()
         tdmap_end=MPI.Wtime()
         if (pk_exist==0):
          print("datamap not exists, scanning all files cost %.2f seconds"%(tdmap_end-tdmap_start))
         else:
          print("datamap exists, loading cost %.2f seconds"%(tdmap_end-tdmap_start))
##_______________ Create File ______________##   

        if rank==0 and (template==1 or template==2):
           temp_start=MPI.Wtime()
           try:
            create_template(outfile,global_fiber,datamap1,'fiber',rank)
           except Exception as e:
            traceback.print_exc()
           temp_end=MPI.Wtime()
           print("template creation cost %.2f"%(temp_end-temp_start))
        tcreated=MPI.Wtime()
##__________________ I/O ___________________## 
        if template ==0 or template==2: 
         try: 
          hx = h5py.File(outfile,'a',driver='mpio', comm=MPI.COMM_WORLD) ## collectively open file 
          hx.atomic=False 
         except Exception as e:
          traceback.print_exc()        
        topen=MPI.Wtime()
        tclose=topen        
        fiber_copyte=topen
        fiber_copyts=topen
        catalog_copyts=topen
        catalog_copyte=topen
        if template==0 or template==2:
           fiber_copyts=MPI.Wtime()
           overwrite_template(hx,fiber_dict,'fiber')
           fiber_copyte=MPI.Wtime()
           #print("rank:%d\tlength:%d\tcost:%.2f"%(rank,fiber_item_length,fiber_copyte-fiber_copyts))
           #for each fiber, find the catalog, then copy it
           #catalog_copyts=MPI.Wtime()
           #overwrite_template(hx,catalog_dict,'catalog')
           #catalog_copyte=MPI.Wtime()
           catalog_copyts=0
           catalog_copyte=0
           comm.Barrier()
           tclose_s=MPI.Wtime()
           hx.close()
           tclose=MPI.Wtime()
           #print("rank:%d,fiber cost:%.2f"%(rank,fiber_copyte-fiber_copyts))
           if rank==0:
              print ("Overview after template creation:\n1.SSF File open: %.2f\nFiber copy total: %.2f\nCatalog copy total: %.2f\n SSF File close: %.2f\nTotal Cost(including everything): %.2f"%(topen-tcreated,fiber_copyte-fiber_copyts,catalog_copyte-catalog_copyts,tclose-tclose_s,tclose-tstart))
if __name__=='__main__': 
    parallel_select()
