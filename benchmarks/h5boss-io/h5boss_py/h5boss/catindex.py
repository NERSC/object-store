import numpy as np
import h5py
import time
import traceback
def h5index(infiles):
    '''
      index the fiber column in each catalog table
    '''
    meta=['plugmap', 'zbest', 'zline', 'photo/match', 'photo/matchflux', 'photo/matchpos']
    if not isinstance(infiles, (list, tuple)):
        infiles = [infiles,]
    catatime=0.0
    fopentime=0.0
    for infile in infiles:
        fopen_start=time.time()
        try: 
         fx = h5py.File(infile, mode='a')
        except Exception as e:
         print ("File open error: ",infile)
         pass
        ttopen=time.time()-fopen_start
        fopentime+=ttopen
        plate = fx.keys()[0]
        mjd = fx[plate].keys()[0]
        id=plate+'/'+mjd+'/'
        cataw_start=time.time()
        for i in range(0,len(meta)):
          catid=id+meta[i]
          catidex=catid+'_idx'
          try:
           dset=fx.create_dataset(catidex,dtype=fx[catid]['FIBERID'].dtype,data=fx[catid]['FIBERID'])
          except Exception as e:
           traceback.print_exc()
        ttcata=time.time()-cataw_start
        catatime+=ttcata
        fx.close()           
