import numpy as np
import h5py
import time
import traceback
import fcntl,os 
from multiprocessing import Pool
plates=[]
mjds=[]
fibers=[]
meta=[]
hx=None
select_files=list()
outfile=""
def subselect(infile):
#	tstart=time.time()
        global select_files
        global outfile
        global hx
        print (hx)
#	dwtime=0.0
#	dmwtime=0.0
        print("plates/mjds/fiber found in: ") 
        hx1=open(outfile,"r+")
        try: 
          hx=h5py.File(outfile,mode='w')
          print ("hx is opened at %.2f"%(time.time()))
          print ("hx is :",hx)
        except Exception as e:
          print("hx open error at time %.2f"%(time.time()))
        fcntl.lockf(hx1,fcntl.LOCK_EX)
        try: 
         fx = h5py.File(infile, mode='r')
         print  ("file %s is opened at %.2f"%(time.time(),infile))
         print ("fx is :",fx)
        except Exception as e:
         print ("File open error: %s "%infile)
        print ("fx.keys():",fx.keys())
        print ("plates:",plates)
        for plate in fx.keys():
         for mjd in fx[plate].keys():
                ii = (plates == plate) & (mjds == mjd)
                xfibers = fibers[ii]
                parent_id='{}/{}'.format(plate, mjd)
                if np.any(ii):
                   print ("now reading file %s"%infile)
                   select_files.append(infile)
                   print ("now the length of selected files is %d"%len(select_files))
                   if parent_id not in hx:
                    hx.create_group(parent_id)
                    print ("group created at %.2f"%(time.time()),parent_id)
#		   dataw_start=time.time() 
                    for fiber in xfibers:
                       id = '{}/{}/{}'.format(plate, mjd, fiber)
                       if id not in hx:
                        try:
                         fx.copy(id, hx[parent_id])
                         print("data object copied at %.2f"%(time.time()),id)
                        except Exception as e:
                         print("fiber %s not found"%id)
                        pass                
#		   dataw_end=time.time()
#		   dwtime+=dataw_end-dataw_start
	  
                    for name in meta:
                       id = '{}/{}/{}'.format(plate, mjd, name)
                       try:
                        catalog = fx[id]
                        yfib=xfibers.astype(np.int32)
                        jj = np.in1d(catalog['FIBERID'], yfib)
                        if id not in hx:
                          hx[id] = fx[id][jj].copy()
		 #        print ("catalog object copied ",id)
                       except Exception as e:
                        print("catalog %s not found"%id)
                        pass
	#	   datamw_end=time.time()
#		   dmwtime+=datamw_end-dataw_end
		#else: 
		#   print ("pmf not found in input file",infile) 
        fx.close()          
        try:
         hx.close()
         print ("hx is closed at %.2f"%(time.time())) 
         print ("hx is :",hx)
        except Exception as e:
         print ("child closing file error")
        fcntl.lockf(hx1,fcntl.LOCK_UN)
	#tend=time.time()-tstart
	#print ('Total selection time: %.2f seconds'%tend)
	#print ('Data read/write time: %.2f seconds'%dwtime)
	#print ('Catalog read/write timne: %.2f seconds'%dmwtime)
	#print ('Metadata operation time: %.2f seconds'%(tend-dwtime-dmwtime))

        
def select(infiles, outfiles, platess, mjdss, fiberss, nproc=None):
    '''
    Select a set of (plates,mjds,fibers) from a set of input files
    
    Args:
        infiles : list of input filenames, or single filename
        outfile : output file to write the selection
        plates : list of plates
        mjds : list of plates
        fibers : list of fibers        
    '''
    global plates,mjds,fibers,meta,hx,select_files
    plates = np.asarray(platess)
    mjds = np.asarray(mjdss)
    fibers = np.asarray(fiberss)
    meta=['plugmap', 'zbest', 'zline',
                        'photo/match', 'photo/matchflux', 'photo/matchpos']
    if not isinstance(infiles, (list, tuple)):
        infiles = [infiles,]
    global outfile
    outfile=outfiles
    #try:
    # hx = h5py.File(outfile,'w',driver='core')
    #except Exception,e:
    # print ("create final file error:%s"%outfile)
    tstart=time.time()
    print ("total hdf5 files %d"%len(infiles))
    if nproc != None and nproc>1:
     print ("Using %d processes"%nproc)
     p=Pool(nproc)
     p.map(subselect, infiles)
     #p.join
    else:
     print ("Using 1 process")
     list(map(subselect,infiles))  
     #subselect(infiles)
    print ("finally ",hx)
    #try: 
    #  hx.close()
    #except Exception,e:
    # print ("final close error")
    # pass
    print("Selected %d files"%len(select_files))
    if(len(select_files)>0):
     selected_f="selected_files_"+str(len(select_files))+".out"
     print("Selected file info saved in %s"%str(selected_f))
     with open(selected_f,"wb") as f:
      f.writelines(["%s\n" % item  for item in select_files])
    print ('Total Cost: %.2f'%(time.time()-tstart))
