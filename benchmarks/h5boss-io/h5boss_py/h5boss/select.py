import numpy as np
import h5py
import time
import traceback
from os.path import basename
import os,sys
'''
 select: Query a list of (plates,mjds,fibers) from source files and produce a single shared output
 sub_select: call select() but produce one output per (plate, mjd,fiber) query, 
             all those subfiles will be managed by a master file
 kv_nodetype: return a key,value list, where key is the dataset link, and value is its type.
 traverse_node: used in kv_nodetype for traversing the hdf5 group hierarchicy and reach the endpoint, i.e., dataset 
'''

fx=""
fiberdatalink={}
pid=""
cata_create=0.0
cata_resize=0.0
get_cata=0.0
dest_cata_read=0.0
src_cata_read=0.0
meta=['plugmap', 'zbest', 'zline', 'photo/match', 'photo/matchflux', 'photo/matchpos']

def traverse_node(name):
    global fx,pid,fiberdatalink
    try:
     cur_node=name.encode('ascii','ignore')
     node=pid+'/'+cur_node
     node_t=str(type(fx[node]))
     if 'dataset' in node_t: 
     # this means we find a dataset node, which must be an endpoint in its group hierarchy, 
     #we don't need to record the group information, as the path to the dataset already contains path to the groups.
        node_t=fx[node].dtype
        fiberdatalink[node]=node_t
    except Exception as e:
     traceback.print_exc()
     pass

def kv_nodetype(infile,plates,mjds,fibers):
        global pid,fiberdatalink,fx
        try:
         fx = h5py.File(infile, mode='r')
         # (kev,value) dictionary for caching (plates/mjd/fiber/../dataset, dataset type)
         # python dict's updating can ensure that the key is unique, i.e., plate/mjd/fiber/../dataset is unique
         fiberlink={}
         for plate in fx.keys():
            for mjd in fx[plate].keys():
                ii = (plates == plate) & (mjds == mjd)
                xfibers = fibers[ii]
                if np.any(ii): # fiber is found
                  for fiber in xfibers:#for each fiber node, recursively visit its members and record the 
                      #fiberlink={id:infile}
                      pid = '{}/{}/{}'.format(plate, mjd, fiber)
                      fx[pid].visit(traverse_node)
         fx.close()
        except Exception as e:
         print (pid)
         traceback.print_exc()
         print (pid,infile)
         pass
        return (fiberdatalink)
def sub_select(infile,plates,mjds,fibers,masterfile,rank,id):
    master_dir=os.path.dirname(os.path.realpath(masterfile))+'/'+os.path.basename(masterfile).split('.')[0]

    slavefile=master_dir+'/'+str(rank)+'_'+str(id)+'.h5'
    try:
      select(infile, slavefile, plates, mjds, fibers)
    except Exception as e:
      print ("Error in slave file:%s")
      traceback.print_exc()

def fiber_copy(xfibers,fx,hx,parent_id, plate, mjd):
#Step 1: copy a fiber object, count the time: data_copy
 for fiber in xfibers:
  id = '{}/{}/{}'.format(plate, mjd, fiber)
  if id not in hx:
   try:
    fx.copy(id, hx[parent_id])
   except Exception as e:
    pass

def catalog_copy(xfibers,fx,hx,plate,mjd):
 global cata_create, meta,cata_resize,dest_cata_read,get_cata,src_cata_read
 for name in meta:
  id = '{}/{}/{}'.format(plate, mjd, name)
  try:  
   get_cata_start=time.time()
   yfib=xfibers.astype(np.int32)
   # copy all catalog metadata
   temp_cata_fiber_all=fx[id]
   #temp_cata_fiber_col=fx[id]['FIBERID']
   src_cata_read+=time.time()-get_cata_start
   #jj = np.in1d(temp_cata_fiber_col,yfib)
   jj = np.in1d(temp_cata_fiber_all['FIBERID'],yfib)
   get_cata+=time.time()-get_cata_start
   cataid_start_time=time.time()
   if id not in hx:
    cata_create_start=time.time()
    #dset=hx.create_dataset(id,maxshape=(None,),dtype=fx[id][jj].dtype,data=fx[id][jj])
    dset=hx.create_dataset(id,maxshape=(None,),dtype=temp_cata_fiber_all[jj].dtype,data=temp_cata_fiber_all[jj])
    cata_create+=time.time()-cata_create_start
   else:
    print("cata id in outputfile")
    if fx[id][jj]['FIBERID'] not in hx[id]['FIBERID']:
     print("fiberid in fx but not in output file")
     cata_read_start=time.time()
     dset=hx[id]
     dest_cata_read+=time.time()-cata_read_start
     cata_resize_start=time.time()
     dset.resize(len(dset)+1)
     dset[len(dset)-1]=fx[id][jj]
     dset.close()
     cata_resize+=cata_resize_start
  except Exception as e:
   print("catalog %s add error"%id)
   traceback.print_exc()
   pass

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
    if not isinstance(infiles, (list, tuple)):
        infiles = [infiles,]
    plates = np.asarray(plates)
    mjds = np.asarray(mjds)
    fibers = np.asarray(fibers) 
    global meta, cata_create, dest_cata_read, cata_resize, get_cata,src_cata_read
 
    cata_copy=0.0 
    fopentime=0.0
    data_copy=0.0

    tstart=time.time()
    for infile in infiles:
        fopen_start=time.time()
        try: 
         fx = h5py.File(infile, mode='r')
        except Exception as e:
         print ("File open error: ",infile)
         continue
        ttopen=time.time()-fopen_start
        fopentime+=ttopen
        for plate in fx.keys():
            iplate = (plates == plate)
            if not np.any(iplate):
              continue
            for mjd in fx[plate].keys():
                ii = (plates == plate) & (mjds == mjd)
                xfibers = fibers[ii]
                parent_id='{}/{}'.format(plate, mjd)
                if not np.any(ii):
                   continue
                else:
                   #open single shared output file
                   try:
                    hx = h5py.File(outfile,'a')
                   except:
                    pass
                   #create new group in output file
                   if parent_id not in hx:
                    hx.create_group(parent_id)
                   #Step 1: copy a fiber object, count the time: data_copy
                   actual_copy_time_start=time.time()                  
                   fiber_copy(xfibers,fx,hx,parent_id, plate, mjd)
                   data_copy+=time.time()-actual_copy_time_start                  
                   #Step 2: copy a catalog row from catalog table, 
                    #report the time: cata_copy:read catalog fiber column, search entry in that column, copy catalog row
                   cataw_start=time.time()
                   catalog_copy(xfibers,fx,hx,plate,mjd)
                   cata_copy+=time.time()-cataw_start
        fx.close()           
    try:
     hx.close()
    except:
     pass
    tend=time.time()-tstart
    #print ('1. Source file open time: %.2f seconds, %.2f of total cost'%(fopentime,fopentime/tend))
    #print ('2. Fiber object copy time: %.2f seconds,%.2f of total cost'%(data_copy,data_copy/tend))
    #print ('3. Catalog table copy time: %.2f seconds,%.2f of total cost'%(cata_copy,cata_copy/tend))
    #print ('3.1: Read the fiber column in catalog table: %.2f, %.2f of cata copy cost'%(src_cata_read,src_cata_read/cata_copy))
    #print ('3.2: Search the fiber entries in fiber column: %.2f, %.2f of cata copy cost'%((get_cata-src_cata_read),(get_cata-src_cata_read)/cata_copy))
    #print ('3.3: catalog row(s) copy time: %.2f, %.2f of cata copy cost'%(cata_create,cata_create/cata_copy))
    #print ('1+2+3:Total time: %.2f'%tend)
    #print ('Verify: 1,2,3 vs Total time: %.2f vs %.2f'%((fopentime+data_copy+cata_copy),tend))
    #print ('Verify: 3.1, 3.2, 3.3 vs Catalog table copy time: %.2f vs %.2f'%((get_cata+cata_create),cata_copy))

    #in case of parallel output
    print ('Source: %.2f'%(fopentime))
    print ('Fiber: %.2f'%(data_copy))
    print ('Catalog: %.2f'%(cata_copy))
    print ('column: %.2f'%(src_cata_read))
    print ('entries: %.2f'%((get_cata-src_cata_read)))
    print ('row: %.2f'%(cata_create))
    print ('Total: %.2f'%tend)
    #print ('Verify: 1,2,3 vs Total time: %.2f vs %.2f'%((fopentime+data_copy+cata_copy),tend))
    #print ('Verify: 3.1, 3.2, 3.3 vs Catalog table copy time: %.2f vs %.2f'%((get_cata+cata_create),cata_copy))

if __name__ == '__main__':
    select(infiles, outfile, plates, mjds,fibers)
