import numpy as np
import h5py
import time
import traceback
import commands
def select(infiles, outfile, plates, mjds,fibers):
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
#        print ("file open time:%.2f,%s"%(ttopen,infile))
        for plate in fx.keys():
            for mjd in fx[plate].keys():
                ii = (plates == plate) & (mjds == mjd)
                xfibers = fibers[ii]
                if np.any(ii):
                   dataw_start=time.time() 
                   for fiber in xfibers:
                       id = '{}/{}/{}'.format(plate, mjd, fiber)
                       #if id not in hx:
                       try:
                         cmdstr="h5copy -i %s -o %s -s %s -d %s -p 2>/dev/null"%(infile,outfile,id,id)
                         (istatus, ioutput)=commands.getstatusoutput(cmdstr)
                         if istatus!=0:
                            print ("error in cmdstr:%s"%ioutput)
                            print (cmdstr)
                       except Exception as e:
                         pass 
                   tt=time.time()-dataw_start
#                   print ("data copy time:%.2f,%s,%s"%(tt,id,infile))            
                   print ("%.2f, %s"%(tt,infile))  
                   dwtime+=tt   
                   
        fx.close()           
    try:
     hx.close()
    except:
     pass
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
#    print ('cata copy time: %.2f seconds'%(catatime))
    print ('source file open time: %.2f seconds'%fopentime)
#    print ('plates/mjd search time: %.2f seconds'%pmsearchtime)
#    print ('output file create time: %.2f seconds'%output_createtime)
#    print ('output group create time: %.2f seconds'%output_grouptime)
#    print ('cata read all time: %.2f'%cata_all_read)
#    print ('search a row in cata: %.2f'%get_cata)
#    print ('cata create time: %.2f'%cata_create)
#    print ('cata read time: %.2f'%cata_read)
#    print ('cata resize tiem%.2f'%cata_resize)
    print ('Selection time: %.2f'%tend)
