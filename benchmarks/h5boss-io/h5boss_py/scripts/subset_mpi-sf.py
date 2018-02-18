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
from h5boss.select import * 
import sys,os
import time
import optparse
import csv
import traceback
#import pandas as pd
import numpy as np
import optparse
import argparse
from collections import defaultdict

meta=['plugmap', 'zbest', 'zline',
                        'photo/match', 'photo/matchflux', 'photo/matchpos']
def list_csv(x):
    columns = defaultdict(list) # each value in each column is appended to a list
    try:
     with open(x) as f:
      reader = csv.DictReader(f,delimiter=' ') # read rows into a dictionary format
      for row in reader: # read a row as {column1: value1, column2: value2,...}
        for (k,v) in row.items(): # go over each column name and value 
            columns[k].append(v) # append the value into the appropriate list
                                 # based on column name k
    except Exception as e: 
     print ("read pmf csv error")
     traceback.print_exc()
     sys.exit()
    return columns
def parse_pmf(input,output,pmflist,rank):
    '''
        input:   HDF5 files list, i.e., source data
        output:  HDF5 file, to be created or updated
        pmflist: Plates/mjds/fibers numbers to be quried
        
        This function is to check the input/output and pmflist
        return plates, mjds, fibers as separate numpy arrays 
    '''
    # check output file and its path
    if os.path.exists(output):
        if rank==0:
         print ("The output file %s is existed, your job is going to overwrite it or update it"%output)
    elif os.access(os.path.dirname(output),os.W_OK):
        if rank==0:
         print ("The output file %s is not existed, your job will create a new file"%output)
    else:
	if rank==0:
         print ("The output file's path does not exist, job exits now")
        sys.exit()
        
    # parse plates/mjds/fibers    
    plates=[]
    mjds=[]
    fibers=[]
    try:
        df = list_csv(pmflist)
        plates = df['plates']
        mjds = df['mjds']
        fibers = df['fibers']
    except Exception as e:
        print("pmflist csv read error or not exist:%s"%e,pmflist)
        traceback.print_exc()
        print("Note: 1st row of csv should start with 'plates mjds fibers'")
    if len(plates)==0:
        print ("No query is found, plate is empty")
        sys.exit()
   
    try:
        with open(input,'rt') as f:
         reader = csv.reader(f)
         hdfsource = list(reader)
        hdfsource = [x for sublist in hdfsource for x in sublist]
    except Exception as e:
        print ("HDF5 inputlist csv read error or not exist: %s"%e,input)

    if(len(hdfsource)==0):
        print("HDF5 source is empty")
        sys.exit(0)
     
    plates = np.asarray(plates)
    mjds = np.asarray(mjds)
    fibers = np.asarray(fibers)
  
    return (plates,mjds,fibers,hdfsource)

def parallel_select():
    '''
    Select a set of (plates,mjds,fibers) from the realesed BOSS data in HDF5 formats.
    
    Args:
        input:   HDF5 files list, i.e., source data, [csv file]
        output:  HDF5 file, to be created or updated
        pmf: Plates/mjds/fibers numbers to be quried, [csv file]
       
    '''
    parser = argparse.ArgumentParser(prog='subset')
    parser.add_argument("input",  help="HDF5 input list")
    parser.add_argument("master", help="HDF5 output master file")
    parser.add_argument("pmf",    help="Plate/mjd/fiber list")
    parser.add_argument("--mpi", help="using mpi yes/no")
    opts=parser.parse_args()

    infiles = opts.input
    masterfile = opts.master
    pmflist = opts.pmf
    global meta

    if opts.mpi is None or opts.mpi=="no": 
        #starts seirial processing
        print ("Try the subset.py or subset command")
        sys.exit()
    elif opts.mpi and opts.mpi=="yes":
        comm =MPI.COMM_WORLD
        nproc = comm.Get_size()
        rank = comm.Get_rank()
        (plates,mjds,fibers,hdfsource) = parse_pmf(infiles, masterfile, pmflist,rank)
    	if rank==0:
     	 print ("HDF5 source: %d files:"%len(hdfsource))
     	 print ("Output: master file: %s "%masterfile)
	 plates_uni_array = np.unique(np.asarray(plates))
     	 print ("Number of plates to be quired: %d; and %d uniquely"%(plates.size,plates_uni_array.size))        
        #collectively open the output file
        master_dir=os.path.dirname(os.path.realpath(masterfile))+'/'+os.path.basename(masterfile).split('.')[0]
        if rank==0:
           try:
            os.stat(master_dir)
           except:
            os.mkdir(master_dir)  
        comm.Barrier()
        try:
             hx = h5py.File(masterfile,'w',driver='mpio', comm=MPI.COMM_WORLD)
        except Exception as e:
             print ("Output file creat error:%s"%masterfile)
	     traceback.print_exc()
        comm.Barrier()
        tstart=time.time()
        if rank==0: print ("Number of processes %d"%nproc)

        #each rank gets a subset of the filelist
        total_files=len(hdfsource)
        #distribute the workload evenly to each process
        step=total_files / nproc
        rank_start =int( rank * step)
        rank_end = int(rank_start + step)
        if(rank==nproc-1):
            rank_end=total_files # adjust the last rank's range
        range_files=hdfsource[rank_start:rank_end]
 
        for i in range(0,len(range_files)):
            sub_select(range_files[i],plates,mjds,fibers,masterfile,rank,i)
        comm.Barrier() 
        try:
             hx.close()
        except Exception as e:
             print ("Master file closing error:%s"%outfile)
             traceback.print_exc()
        if rank==0:
            print ('Cost: %.2f'%(time.time()-tstart))

if __name__=='__main__': 
    parallel_select()
