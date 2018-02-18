import numpy as np
import h5py
import time
import traceback
from h5boss.pmf import pmf
from h5boss.select import select
from h5boss.remove import remove
def select_update(infiles, infile, plates, mjds, fibers):
    '''
    add the missing (plates,mjds,fibers) from a set of input files
    to the pre-existing subset file
    remove the additional (plates, mjds, fibers) from the pre-existing file

    Args:
        infiles : list of input filenames, or single filename
        infile : pre-existing subset file
        plates : list of plates
        mjds : list of plates
        fibers : list of fibers        
    '''
    miss=[]
    left=[]
    try:
     miss, left = pmf(infile,plates,mjds,fibers)
    except Exception, e:
     print ('Metadata checking error')
     traceback.print_exc()
    miss2=[]
    left2=[]
    if(len(miss)>0):
     print ("%d to be added"%len(miss))
     miss2=miss.view(miss.dtype[0]).reshape(miss.shape+(-1,))[:,0]
     try:
      plate = miss2[:,0]
      mjd = miss2[:,1]
      fiber = miss2[:,2]
      print ('plates/mjds/fibers to be added in %s'%infile)
      print plate
      print mjd
      print fiber
      select(infiles,infile,plate,mjd,fiber)
     except Exception, e:
      print ('Error in adding new pmf to the file')
    else: 
     print ("Nothing to be added")

    if(len(left)>0):
     print ("%d to be removed"%len(left)) 
     left2=left.view(left.dtype[0]).reshape(left.shape+(-1,))
     try:
      print ('plates/mjds/fibers to be removed in %s'%infile)
      dplate = left2[:,0]
      dmjd = left2[:,1]
      dfiber = left2[:,2]
      remove(infile, dplate,dmjd,dfiber) 
     except Exception, e:
      print ('Error in removing pmf in the file')
    else: 
      print ("Nothing to be removed")
