import h5py
import numpy as np
from mpi4py import MPI
import sys
import csv
def getfibers(x):
   try:
    fx=h5py.File(x,'r')
   except Exception,e:
    print ('file error')
   plate=[]
   mjd=[]
   fibers=[]
   try: 
    plate=str(fx.keys()[0])
    mjd=str(fx[plate].keys()[0])
    pm=plate+'/'+mjd
    fibers=fx[pm].keys()
   except Exception,e:
    print ('cant find plates/mjds/fibers in:%s'%x)
   #print ("plates:"+plate+"mjds:"+mjd)
   try: 
    fibers.remove('photo')
    fibers.remove('plugmap')
    fibers.remove('zbest')
    fibers.remove('zline') 
   except Exception,e:
    print ('cant remove meta in: %s'%x) 
   try:
    fibers=map(int,fibers)  
    platei=int(plate)
    mjdi=int(mjd)
    for item in fibers:
      print platei,mjdi,item
   except Exception,e:
    print ('last step print error:%s'%x)
   try: 
    fx.close()
   except Exception,e:
    print('file close error:%s'%x)

def mpigetfiber(inputlist):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if (rank==0):
     print ('total ranks:%d'%rank)
     print ('total files:%d'%len(inputlist))
    try:
     if(rank+1000<len(inputlist)):
      cur_file=inputlist[rank+1000]
      getfibers(cur_file) 
    except Exception,e:
     print ("errror in rank:%d"%rank)
if __name__ == "__main__":
   input=sys.argv[1]
   infile=[]
   try:
    with open(input,'rb') as f:
     reader = csv.reader(f)
     infile = list(reader)

   except Exception, e:
    print (" file open error: %s"%e,input)
   infile = [x for sublist in infile for x in sublist] 
   mpigetfiber(infile)
    
