import numpy as np
import h5py
import time
import sys,os
from h5boss.pmf import pmf
import commands
from h5boss.select import select
def remove(infile, plates, mjds, fibers,repack=None):
    '''
    remove the additional (plates, mjds, fibers) from the pre-existing file

    Args:
        infile : pre-existing subset file
        plates : list of plates
        mjds : list of plates
        fibers : list of fibers        
    '''
    meta=['plugmap', 'zbest', 'zline',
                        'photo/match', 'photo/matchflux', 'photo/matchpos']
    try: 
      fx=h5py.File(infile,'a')
    except Exception, e:
      print ('File open error in removing')
    try:
     k=0
     j=0
     print (len(plates))
     #plates=plates.reshape(plates.shape+(-1,))
     print plates[0]
     for i in range(0,len(plates)):
      a=str(plates[i])+'/'+str(mjds[i])+'/'+str(fibers[i])
      print a
      if a in fx:
        fx.__delitem__(a)
        k+=1
      else:
       j+=1
      # meta=['plugmap', 'zbest', 'zline',
      #                  'photo/match', 'photo/matchflux', 'photo/matchpos']
      #remove entries in photo, plugmap, etc
     print ('Removed %d pmf,Skipped %d'%k%j)
     fx.close()
    except Exception, e:
     print ('Error in removing')
    if repack!=None: 
      #run hdf5 repack utility from 
      cmd_moduleload = "module load cray-hdf5 >/dev/null"
      oufile=infile.split('.')[0]+"_repack"+".h5"
      cmd_repack = "h5repack %s %s 2>/dev/null"%(infile,oufile)
      try: 
	commands.getstatusoutput(cmd_moduleload)
	commands.getstatusoutput(cmd_repack)
      except Exception,e:
        print ('Repack error')
