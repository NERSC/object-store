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
exposure_dat=("wave","flux","ivar","mask","wavedisp","sky","x","calib") #exposures
coadd_dat=("wave","flux","ivar","and_mask","or_mask","wavedisp","sky","model") # wavelength
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
    for item in dict2:
      if item not in dict1:
        dict1[item] = dict2[item]
      else: 
        dict1[item][1]=dict1[item][1]+dict2[item][1]
        dict1[item][2]=dict2[item][2]+dict2[item][2]
    return dict1
def add_numpy(dict1, dict2, datatype):
    if(dict2.size==0):
       return dict1
    if(dict1.size==0):
       return dict2
    dict1=np.append(dict1,dict2,axis=0)
    return dict1
def create_template(outfile, global_dict,dmap,choice,rank):
    if choice=='fiber':
      create_fiber_template(outfile,global_dict,dmap,rank)
    elif choice=='catalog':
      create_catlog_template(outfile,global_dict)

def create_fiber_template(outfile,global_dict,dmap,rank):
#use one process to create the template
 try:
     hx = h5py.File(outfile,'a',libver='latest')
 except Exception as e:
     print ("rank:%d, Output file open error:%s"%(rank,outfile))
     traceback.print_exc()
 try:#Set the allocate time as early. --Quincey Koziol
  all_coadd_info=(dmap[0][0],dmap[1])#(type, size_dic)
  all_exp_info=(dmap[0][1],dmap[2],dmap[3])#(type, sizeb,sizer)
  for key,value in global_dict.items():
      fiberlength=len(value[1])
      if fiberlength>0:
        inter_grp=key
        if (inter_grp.split('/')[-1]=="coadds"):
           _fiber_template(hx,inter_grp,fiberlength,all_coadd_info)
        else: 
           _fiber_template(hx,inter_grp,fiberlength,all_exp_info)
#    else:
#     _catalog_template(hx,key,value)
 except Exception as e:
   traceback.print_exc()

 try:
  hx.flush()
  hx.close()
 except Exception as e:
  print("hx close error in %d"%rank)
  traceback.print_exc()
def create_catlog_template(outfile,global_dict):
#use one process to create the template
 try:
     hx = h5py.File(outfile,'a',libver='latest')
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
  hx.flush()
  hx.close()
 except Exception as e:
  print("hx close error in rank0")
  traceback.print_exc()
  pass
def _fiber_template(hx,inter_grp,fiberlength,d_info):
   for dset in range(0,len(exposure_dat)):
     try:
        #cur_dset_name=inter_grp+'/'+dset
        #cur_dset_type=d_info[0][dset][0]
        pm=inter_grp.split('/')[0]+'/'+inter_grp.split('/')[1]
        if inter_grp.split('/')[2]=="coadds":
           dg="coadds"
           cur_dset_shape=d_info[1][pm]
           cur_dset_name=inter_grp+'/'+coadd_dat[dset]
           cur_dset_type=d_info[0][coadd_dat[dset]][0]
        elif inter_grp.split('/')[-1]=="b":
           dg="b"
           cur_dset_shape=d_info[1]
           cur_dset_name=inter_grp+'/'+exposure_dat[dset]
           cur_dset_type=d_info[0][exposure_dat[dset]][0]
        else:
           dg="r"
           cur_dset_shape=d_info[2]
           cur_dset_name=inter_grp+'/'+exposure_dat[dset]
           cur_dset_type=d_info[0][exposure_dat[dset]][0]
        #space=(fiberlength,cur_dset_shape[1])
        space=(fiberlength,cur_dset_shape)
        if dset=='wave' and dg=="coadds":
           space=(cur_dset_shape,)
        spaceid=h5py.h5s.create_simple(space)
        plist=h5py.h5p.create(h5py.h5p.DATASET_CREATE)
        plist.set_alloc_time(h5py.h5d.ALLOC_TIME_EARLY)
        tid=h5py.h5t.py_create(cur_dset_type, False)
     except Exception as e:
        traceback.print_exc()
        print("error in fiber template create:",dset)
        pass
     try:#create intermediate groups
        hx.create_group(inter_grp)
     except Exception as e:
        pass #groups existed, so pass it
     try:
        h5py.h5d.create(hx.id,cur_dset_name,tid,spaceid,plist)#create dataset with property list:early allocate
     except Exception as e:
        print("dataset create error: %s"%cur_dset_name)
        traceback.print_exc()
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

def overwrite_template(hx, data_dict,choice):
 #Read/Write all dataset into final file, 
 #each rank handles one fiber_dict, which contains multiple fiber_item
 if choice=='fiber':
  try:
   for key, value in data_dict.items():
      _copy_fiber(hx,key,value)
  except Exception as e:
   print ("Data read/write error key:%s file:%s"%(key,value[2]))
   traceback.print_exc()
   pass
 elif choice=='catalog':
  try:
   for i in range(0,len(data_dict)):
    pm=data_dict[i][0]
    # key: pm, value: (fiberid, global_offset, infile)    
    values_off=data_dict[i][1]
    for i in range(0,len(values_off)):
      _copy_catalog(hx,pm,values_off[i])
  except Exception as e:
   print ("Data read/write error key:%s file:%s"%(key,value[2]))
   traceback.print_exc()
   pass
def _copy_fiber(hx,key,value):  # key is the inter_group, value has filename, fiberlist, fiberoffsetlist
 try:
  subfx=h5py.File(value[0],'r')
  if key.split('/')[2]=="coadds": # copy datasets in coadds group
     for icoad in coadd_dat:
       idset=key+'/'+icoad
       dx=hx[idset]
       try:
        if icoad!='wave': # wave dataset only has 1 dimensional, and all fiber should entirly copy it. 
        # may replace this for loop with a signle I/O call: subdx=subfx[idset][value[2]] then dx=subdx
#          for ifiber in range(0,len(value[2])):
#           fiber_off=value[2][ifiber]
#           if fiber_off < subfx[idset].shape[0]:
#            subdx=subfx[idset][fiber_off]
#           else:
#            subdx=[0]*subfx[idset].shape[1]
#           dx[ifiber]=subdx
          vlist=value[2]
          vlist.sort()
          if subfx[idset].shape[0]==500:
           #trim vlist, remove fiber>500
           vlist=[v for v in vlist if v<=500]
          if len(vlist)>0:
           try:
            #print (idset, vlist)
            #print (subfx[idset][vlist])
            if len(vlist)==1:
	      dx[:] = subfx[idset][vlist][0]
	    else:
              dx[:]=subfx[idset][vlist]
            #print ("dx:",dx)
           except Exception as e:
            #print ("fx:%s dset:%s dx.shape %s vlist has %s,needs to be fixed"%(value[0],idset,dx.shape,vlist))
            pass
        else:
          subdx=subfx[idset]
          dx[:]=subdx
       except Exception as e:
          print ("infile:%s,group:%s,dataset:%s,shape:%s. outdt:%s,otshape:%s"%(value[0],key,icoad,subdx.shape,idset,dx.shape))
  elif key.split('/')[-1]=="b" or key.split('/')[-1]=="r": # copy datasets in exposures groups
     for iexp in exposure_dat:
       idset=key+'/'+iexp
       dx=hx[idset]
       vlist=value[2]
       vlist.sort()
       if subfx[idset].shape[0]==500:
	#trim vlist, remove fiber>500
        vlist=[v for v in vlist if v<=500]
       if len(vlist)>0:
        try:
         if len(vlist)==1:
	   dx[:]=subfx[idset][vlist][0]
         else:
           dx[:]=subfx[idset][vlist]
        except Exception as e:
	 print ("fx:%s dset:%s vlist has %s,needs to be fixed"%(value[0],idset,vlist))
         pass
       # may replace this for loop with a signle I/O call: subdx=subfx[idset][value[2]] then dx=subdx
#       for ifiber in range(0,len(value[2])):
#         fiber_off=value[2][ifiber]
#         if fiber_off< subfx[idset].shape[0]:
#          subdx=subfx[idset][fiber_off]
#         else:
#          subdx=[0]*subfx[idset].shape[1]
         #print ("group:%s dataset:%s ifiber:%d"%(key,iexp,ifiber))
#         dx[ifiber]=subdx 
  else:
     print ("wrong inter group found%s"%key)
  subfx.close()
 except Exception as e:
  traceback.print_exc()
  print ("read subfile %s error at key:%s value:%s"%(value[0],key,value))
  pass

def _copy_catalog(hx,key,values_off):
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
     hx[id][offset]=fx[id][int(fiber_id)-1]
   except Exception as e:
    traceback.print_exc()
    print ("catacopy:%s error:%d"%(id,int(fiber_id)-1))  
