import traceback
import os.path
import numpy as np
from astropy.io import fits
from astropy.table import Table
import h5py
expdat=("wave","flux","ivar","mask","wavedisp","sky","x","calib") #exposures
dat=("wave","flux","ivar","and_mask","or_mask","wavedisp","sky","model") # wavelength
x1k=0
def load_coadds(platefile, zbestfile=None, run1d=None):
    '''
    Document ...
    '''
    #- Load spPlate data
    fx = fits.open(platefile, memmap=False)
    header   = fx[0].header
    nfiber   = header['NAXIS2']
    fx.close() 
    global x1k
    if (nfiber==1000): 
       x1k=x1k+1
    else:
       print (nfiber,platefile)
datapath = "/global/projecta/projectdirs/sdss/data/sdss/dr12/boss/spectro/redux/v5_7_0/"
#outputpath= "/global/cscratch1/sd/jialin/h5boss-bp/"
outputpath= "/global/cscratch1/sd/jialin/h5boss_v2/"

def listfiles():
     ldir=os.listdir(datapath)
     lldir=[fn for fn in ldir if fn.isdigit()]
     return lldir

def findseed(x):
     fitsfiles = [os.path.join(root, name)
       for root, dirs, files in os.walk(x)
       for name in files
        if name.startswith("spPlate") and name.endswith(".fits")]
     return fitsfiles

def count_fibers():
    platefiles=listfiles()
    print ("number of plate files:%d"%len(platefiles))
    for i in platefiles:
       platepath_for_current_rank = datapath+i
       fk = findseed(platepath_for_current_rank)

       try: 
          load_coadds(fk[0])
          #print (fk)
       except Exception as e:
          traceback.print_exc()
          print ("error in %s"%(fk[0]))
          pass
    global x1k
    print (x1k)
if __name__ == "__main__":
    count_fibers()

