import numpy as np
import h5py
import time,os
import traceback 
from h5boss.pmf import pmf 
from h5boss.select import select
catalog_meta=['plugmap', 'zbest', 'zline',
                        'match', 'matchflux', 'matchpos']
meta=['plugmap', 'zbest', 'zline',
                        'photo/match', 'photo/matchflux', 'photo/matchpos']
kk=1
## Global variables:
## Data structure of fiberdatalink: {key, value_pair()} --->  {path_dataset, (datatype,datashape,filename)}
## For example fiberdatalink['3665/52273/360/coadd']= (V32, $SCRATCH/h5boss/3665-52273.h5)
## Aug 3 2016
## Jialin Liu, jalnliu@lbl.gov
# create_slavefile is used in ../script/subset_mpi-sf.py, which creates multiple sub files 
def create_slavefile(infile,plates,mjds,fibers,masterfile,rank,id):
    master_dir=os.path.dirname(os.path.realpath(masterfile))+'/'+os.path.basename(masterfile).split('.')[0]
     
    slavefile=master_dir+'/'+str(rank)+'_'+str(id)+'.h5'
    try:
      select(infile, slavefile, plates, mjds, fibers)
    except Exception as e:
      print ("Error in slave file:%s")
      traceback.print_exc()
      pass 
def add_dic(dict1,dict2,datatype):
    #for item in dict2:
    #  if item not in dict1:
    #    dict1[item] = dict2[item]
    dict1.update(dict2)
    return dict1
def add_numpy(dict1, dict2, datatype):
    if(dict2.size==0):
       return dict1
    if(dict1.size==0):
       return dict2
    dict1=np.append(dict1,dict2,axis=0)
    return dict1
def create_template(outfile, global_dict,choice,rank):
    if choice=='fiber':
      create_fiber_template(outfile,global_dict,rank)
    elif choice=='catalog':
      create_catlog_template(outfile,global_dict)
def create_fiber_template(outfile,global_dict,rank):
#use one process to create the template
 try:
     #creating the file with 'latest' formating, rather than earliest, will utilize the most state-of-art optimizing, e.g., indexing, and optimal heap storage inside hdf5 file
     hx = h5py.File(outfile,'a',libver="latest")
     #hx = h5py.File(outfile,'a')
 except Exception as e:
     print ("rank:%d, Output file open error:%s"%(rank,outfile))
     traceback.print_exc()
 try:#Set the allocate time as early. --Quincey Koziol 
  for key,value in global_dict.items():
    if key.split('/')[-1] not in catalog_meta:
     #print ("rank:%d is creating template"%rank)
     _fiber_template(hx,key,value,rank)
#    else:
#     _catalog_template(hx,key,value)
 except Exception as e:
   traceback.print_exc()
   pass 
 try:
  hx.flush()
  hx.close()
 except Exception as e:
  print("hx close error in %d"%rank)
  traceback.print_exc()
  pass

def create_catlog_template(outfile,global_dict):
#use one process to create the template
 try:
     hx = h5py.File(outfile,'a')
 except Exception as e:
     print ("Output file creat error:%s"%outfile)
     traceback.print_exc()
 try:#Set the allocate time as early. --Quincey Koziol 
  catalog_dict=global_dict[0] #dict: plate/mjd, number of fiber
  catalog_types=global_dict[1] #dict: meta, (type, shape) 
  for key,value in catalog_dict.items():
      # key=plate/mjd
      # value= number of fibers
      # catalog_types:types of each catalog table
     _catalog_template(hx,key,value,catalog_types)
 except Exception as e:
   traceback.print_exc()
   pass
 try:
  #hx.flush()
  hx.close()
 except Exception as e:
  print("hx close error in rank0")
  traceback.print_exc()
  pass

def _fiber_template(hx,key,value,rank):
   space=h5py.h5s.create_simple(value[1])
   plist=h5py.h5p.create(h5py.h5p.DATASET_CREATE)
   plist.set_alloc_time(h5py.h5d.ALLOC_TIME_EARLY)
   tid=h5py.h5t.py_create(value[0], False)
   try:#create intermediate groups
      hx.create_group(os.path.dirname(key))
   except Exception as e:
      #print ("group existed")
      pass #groups existed, so pass it
   try:
    h5py.h5d.create(hx.id,key,tid,space,plist)#create dataset with property list:early allocate
    #print ("rank:%d created data "%(rank))
    #print (hx.id)
   except Exception as e:
    print("dataset create error: %s"%key)
    pass
def _catalog_template(hx,key,value,catalog_types):
   value=int(value)
   item_shape=(value,1)
   space=h5py.h5s.create_simple(item_shape)
   plist=h5py.h5p.create(h5py.h5p.DATASET_CREATE)
   plist.set_alloc_time(h5py.h5d.ALLOC_TIME_EARLY)
   plist.set_layout(h5py.h5d.CHUNKED)
   ichunk=1
   chunk_shape=(ichunk,1) #Quincey suggests to optimize the ik in H5Pset_istore_k
                          # for controling the btree for indexing chunked datasets
   plist.set_chunk(chunk_shape)
   #catalog_types: [plate/mjd/im],( dtype,dshape)
   for ktype, vtype in catalog_types.items():
    ikey=key+'/'+ktype
    tid=h5py.h5t.py_create(vtype[0], False)
    max_shape=int(vtype[1][0])
    #print(value,vtype)
    assert(value<=max_shape),"value:%d,maxshape:%d"%(value,max_shape) # number of fibers is larger than the maximum catalog size
    try:#create intermediate groups
       hx.create_group(os.path.dirname(ikey))
    except Exception as e:
       #traceback.print_exc()
       pass #groups existed, so pass it
    try:
     h5py.h5d.create(hx.id,ikey,tid,space,plist)#create dataset with property list:early allocate
    except Exception as e:
     print("dataset create error: %s\n"%ikey)
     traceback.print_exc()
     pass

def overwrite_template(hx, data_dict,choice,rwmode):
 #Read/Write all dataset into final file, 
 #each rank handles one fiber_dict, which contains multiple fiber_item
 if choice=='fiber':    
  try:
   for key, value in data_dict.items():
    if key.split('/')[-1] not in catalog_meta:
      _copy_fiber(hx,key,value,rwmode)
  except Exception as e:
   print ("Data read/write error key:%s file:%s"%(key,value[2]))
   #traceback.print_exc()
   pass
 elif choice=='catalog':
  try:
   for i in range(0,len(data_dict)):
    pm=data_dict[i][0]
    # key: pm, value: (fiberid, global_offset, infile)    
    values_off=data_dict[i][1]
    for i in range(0,len(values_off)): 
      _copy_catalog(hx,pm,values_off[i],rwmode)
  except Exception as e:
   print ("Data read/write error key:%s file:%s"%(key,value[2]))
   traceback.print_exc()
   pass
#@profile
def _copy_fiber(hx,key,value,rwmode):
 try:
  #start=time.time()
  subfx=h5py.File(value[2],'r')
  #subdx=subfx[key].value
  subdx=subfx[key][()]
  #print (subdx.nbytes)
	  #subdx=subfx[key][()]
  #sum=subdx[10][2]
  #for i in range(0,len(subdx)):
  # print(subdx[i][1])
  subfx.close()
  #print ("%.6f"%(time.time()-start))
 except Exception as e:
  traceback.print_exc()
  print ("read subfile %s error"%value[2])
  pass
 if rwmode=="rw":
  try:
  #start=time.time()
   dx=hx[str(key)]
  #xxx=subdx[3]
  #dx[:]
   dx[:]=subdx   #overwrite the existing template data
#  print "fake write"
  #dx[:]=subfx[key].value
  #print ("%.6f"%(time.time()-start))
  except Exception as e:
   traceback.print_exc()
   print ("overwrite error")
   pass

def _copy_catalog(hx,key,values_off,rwmode):
   try:
    fx=h5py.File(values_off[1],'r')
    plate=key.split('/')[0]
    mjd=key.split('/')[1]
    fiber_id=values_off[0]
    global_offset=values_off[2]
    for name in meta:
     id = '{}/{}/{}'.format(plate,mjd,name)
     offset=int(global_offset)
     maxoff=hx[id].shape[0]-1
     if offset>maxoff:
       offset=maxoff
     if rwmode=="rw":
      hx[id][offset]=fx[id][int(fiber_id)-1]
     else:# read only, no write to template
      ddx=fx[id][int(fiber_id)-1]
   except Exception as e:
    traceback.print_exc()
    print ("catacopy:%s error:%d"%(id,int(fiber_id)-1))  
