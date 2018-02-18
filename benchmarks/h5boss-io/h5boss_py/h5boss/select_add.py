import numpy as np
import h5py
import time
from h5boss.pmf import pmf
from h5boss.select import select
def select_add(infiles, infile, plates, mjds, fibers):
    '''
    add the missing (plates,mjds,fibers) from a set of input files
    to the pre-existing subset file
    Args:
        infiles : list of input filenames, or single filename
        infile : pre-existing subset file
        plates : list of plates
        mjds : list of plates
        fibers : list of fibers        
    '''

    miss, left = pmf(infile,plates,mjds,fibers)
    miss_array=[]
    try: 
     miss_array=miss.view(miss.dtype[0]).reshape(miss.shape+(-1,))[:,0]
    except Exception,e:
     print ("empty list detected in miss_pmf")
    plate=[]
    mjd=[]
    fiber=[]
    if(len(miss_array)>0):
     plate = miss_array[:,0]
     mjd = miss_array[:,1]
     fiber = miss_array[:,2]
     select(infiles,infile,plate,mjd,fiber)
