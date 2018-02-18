import numpy as np
import h5py
import time
import traceback
#def select(infiles, outfile, plates, mjds, fibers,nproc):
def select(infiles, outfile, plates, mjds,fibers):
    '''
    Select a set of (plates,mjds,fibers) from a set of input files
    
    Args:
        infiles : list of input filenames, or single filename
        outfile : output file to write the selection
        plates : list of plates
        mjds : list of plates
        fibers : list of fibers        
    '''
    plates = np.asarray(plates)
    mjds = np.asarray(mjds)
    fibers = np.asarray(fibers) 
    meta=['plugmap', 'zbest', 'zline', 'photo/match', 'photo/matchflux', 'photo/matchpos']
    if not isinstance(infiles, (list, tuple)):
        infiles = [infiles,]
    tstart=time.time() 
    select_files=list() 
    dwtime=0.0
    catatime=0.0 
    fopentime=0.0
    pmsearchtime=0.0
    output_createtime=0.0
    output_grouptime=0.0
    cata_create=0.0
    cata_read=0.0
    cata_resize=0.0
    get_cata=0.0
    cata_all_read=0.0
    print ("starts the loop in select:",time.time())
    for infile in infiles:
        fopen_start=time.time()
        try: 
         fx = h5py.File(infile, mode='r')
        except Exception as e:
         print ("File open error: ",infile)
         continue
        ttopen=time.time()-fopen_start
        fopentime+=ttopen
        #print ("file open time:%.2f,%s"%(ttopen,infile))
        for plate in fx.keys():
            for mjd in fx[plate].keys():
                pm_search_start=time.time()
                ii = (plates == plate) & (mjds == mjd)
                xfibers = fibers[ii]
                pmsearchtime+=time.time()-pm_search_start
                parent_id='{}/{}'.format(plate, mjd)
                if np.any(ii):
                   #select_files.append(infile)
                   output_create_start=time.time()
                   try:
                    #hx = h5py.File(outfile,'w',driver='core',backing_store=False)
                    #hx = h5py.File(outfile,'w',driver='core')
                    hx = h5py.File(outfile,'w')
                   except:
                    pass
                   output_createtime+=time.time()-output_create_start
                   output_group_start=time.time()
                   if parent_id not in hx:
                    hx.create_group(parent_id)
                   output_grouptime+=time.time()-output_group_start
                   dataw_start=time.time() 
                   for fiber in xfibers:
                       id = '{}/{}/{}'.format(plate, mjd, fiber)
                       if id not in hx:
                        try:
			 #this is the object copy
                         fx.copy(id, hx[parent_id])
                         #sfiber=fx[id].keys()
                         #print (id,sfiber)
                         #print (fx[id].keys())
                         #dx_coadd=id+'/coadd'
                         #expid=id+'/exposures'
                         #expids = fx[expid].keys()
                         #d_coadd=fx[dx_coadd][:]
                         #for i in expids:
                         #  xb=expid+'/'+i+'/b'
                         #  xr=expid+'/'+i+'/r'
                         #  d_exp_xb=fx[xb][:]
                         #  d_exp_xr=fx[xr][:]
                        except Exception as e:
                         #traceback.print_exc()
                         #print("fiber %s not found"%id)
                         pass 
                   tt=time.time()-dataw_start
                   #print ("data copy time:%.2f,%s,%s"%(tt,id,infile))            
                   dwtime+=tt   
                   cataw_start=time.time()
                   for name in meta:
                       id = '{}/{}/{}'.format(plate, mjd, name)
                       try:
                        cata_all_read_start=time.time()
                        #catalog = fx[id]
                        cata_all_read+=time.time()-cata_all_read_start
                        get_cata_start=time.time()
                        yfib=xfibers.astype(np.int32)
                        #jj = np.in1d(catalog['FIBERID'], yfib)
                        
                        jj = np.in1d(fx[id+'_idx'],yfib)
                        get_cata+=time.time()-get_cata_start
                        if id not in hx:
                         cata_create_start=time.time()
                         dset=hx.create_dataset(id,maxshape=(None,),dtype=fx[id][jj].dtype,data=fx[id][jj])
			 #this is the dataset slice copy,i.e., only copy the row that has the queried fiberid
                         #hx[id] = fx[id][jj].copy()
                         cata_create+=time.time()-cata_create_start
                        else:
                         if fx[id][jj]['FIBERID'] not in hx[id]['FIBERID']:
                          cata_read_start=time.time()
                          dset=hx[id]
                          cata_read+=time.time()-cata_read_start
                          cata_resize_start=time.time()
                          dset.resize(len(dset)+1)
                          dset[len(dset)-1]=fx[id][jj]
                          dset.close()
                          cata_resize+=cata_resize_start
			#exist_id=hx[id] # catalog table is existed, need to update
                       except Exception as e:
                        print("catalog %s add error"%id)
                        traceback.print_exc()
                        pass
                   ttcata=time.time()-cataw_start
                   catatime+=ttcata
                   #print("cata copy time:%.2f,%s,%s"%(ttcata,id,infile))
        fx.close()           
    try:
     #hx.flush()
     hx.close()
    except:
     pass

    #if(len(select_files)>0):
    # selected_f="selected_files_"+str(len(select_files))+".out"
    # with open(selected_f,"wt") as f:
    #  f.writelines(["%s\n" % item  for item in select_files])
    tend=time.time()-tstart
    #print ('%.2f'%dwtime)
    #print ('%.2f'%catatime)
    #print ('%.2f'%fopentime)
    #print ('%.2f'%pmsearchtime)
    #print ('%.2f'%output_createtime)
    #print ('%.2f'%output_grouptime)
    #print ('%.2f'%tend)
    #print ('%.2f'%cata_create)
    #print ('%.2f'%cata_read)
    #print ('%.2f'%cata_resize)
    #print ('Selection time: %.2f seconds'%tend)
    print ('Data copy time: %.2f seconds'%dwtime)
    print ('cata copy time: %.2f seconds'%(catatime))
    print ('source file open time: %.2f seconds'%fopentime)
    print ('plates/mjd search time: %.2f seconds'%pmsearchtime)
    print ('output file create time: %.2f seconds'%output_createtime)
    print ('output group create time: %.2f seconds'%output_grouptime)
    print ('cata read all time: %.2f'%cata_all_read)
    print ('search a row in cata: %.2f'%get_cata)
    print ('cata create time: %.2f'%cata_create)
    print ('cata read time: %.2f'%cata_read)
    print ('cata resize tiem%.2f'%cata_resize)
    print ('Selection time: %.2f'%tend)
