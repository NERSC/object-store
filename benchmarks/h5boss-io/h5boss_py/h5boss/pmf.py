import numpy as np
import h5py
import time
import sys
import os
import csv
import traceback
from collections import defaultdict
fx=""
pid=""
inputfile=""
fiberdatalink={}
cataloglink={}
meta=['plugmap', 'zbest', 'zline',
                        'photo/match', 'photo/matchflux', 'photo/matchpos']

def pmf(infile, plates, mjds, fibers):
    '''
    check (plates,mjds,fibers) in the source hdf5 file
    return the matched/missed pmf

    Args:
        infile : input file
        plates : list of plates
        mjds : list of plates
        fibers : list of fibers        
    '''
    tstart=time.time()
    if(len(plates)==0 or len(mjds)==0 or len(fibers)==0 or len(infile)==0):
      print('input is empty')
      sys.exit(0)
    plates = np.asarray(plates).reshape(len(plates),1)
    mjds = np.asarray(mjds).reshape(len(mjds),1)
    fibers = np.asarray(fibers).reshape(len(fibers),1)
    pm=np.concatenate((plates,mjds),axis=1)
    pmf=np.concatenate((pm,fibers),axis=1)
    inx=h5py.File(infile,'r')
    in_pmf=[]
    notin_pmf=[]
    for pid in inx.keys():
	for mid in inx[pid].keys():
		for fid in inx[pid+'/'+mid].keys():
			if fid.isdigit():
			 a=[str(pid),str(mid),str(fid)]
			 #print a
		         if a in pmf.tolist():
			    #print ('in')
			    in_pmf.append(a)
			 else:
			    #print ('not in')
			    notin_pmf.append(a)	
			
    #notin_pmf contains pmf that is in in pre-existing subset but not in new list
    in_pmf=np.asarray(in_pmf)
    notin_pmf=np.asarray(notin_pmf)
    tend=time.time()-tstart
 
    if(len(in_pmf)>0):
     in_pmf1d=np.core.records.fromarrays(in_pmf.transpose(),names='col1, col2, col3',formats='a25,a25,a25')    
     pmf1d=np.core.records.fromarrays(pmf.transpose(),names='col1, col2, col3',formats='a25,a25,a25')
     missing_pmf=np.setdiff1d(pmf1d,in_pmf1d)
     missing_pmf=missing_pmf.reshape(len(missing_pmf),1)
     print "Fibers found in pmf list, but not in the pre-existing file: %d"%len(missing_pmf)
    else: 
      missing_pmf=np.empty(0)
    if(len(notin_pmf)>0):
     notin_pmf=np.core.records.fromarrays(notin_pmf.transpose(),names='col1, col2, col3',formats='a25,a25,a25')
     print "Fibers found in the pre-existing file, but not in the pmf list: %d"%len(notin_pmf)
    else:
     notin_pmf=np.empty(0)
    print ('Metadata query time: %.2f seconds'%tend)
    return (missing_pmf, notin_pmf)

def parse_csv(input,output,pmflist,rank):
    '''
        Check the input/output and pmflist
        return plates, mjds, fibers as separate numpy arrays 
    Args:
        input:   HDF5 files list, i.e., source data
        output:  HDF5 file, to be created or updated
        pmflist: Plates/mjds/fibers numbers to be quried
    '''
    # check output file and its path
    if os.path.exists(output):
        if rank==0:
         print ("The output file %s is existed, your job is going to overwrite it or update it"%output)
    elif os.access(os.path.dirname(output),os.W_OK):
        if rank==0:
         print ("The output file %s is not existed, your job will create a new file"%output)
    else:
        if rank==0:
         print ("The output file's path does not exist, job exits now")
        sys.exit()

    # parse plates/mjds/fibers    
    plates=[]
    mjds=[]
    fibers=[]
    try:
        df = list_csv(pmflist)
        plates = df['plates']
        mjds = df['mjds']
        fibers = df['fibers']
    except Exception as e:
        print("pmflist csv read error or not exist:%s"%e,pmflist)
        traceback.print_exc()
        print("Note: 1st row of csv should start with 'plates mjds fibers'")
    if len(plates)==0:
        print ("No query is found, plate is empty")
        sys.exit()
    
    try:
        with open(input,'rt') as f:
         reader = csv.reader(f)
         hdfsource = list(reader)
        hdfsource = [x for sublist in hdfsource for x in sublist]
    except Exception as e:
        print ("HDF5 inputlist csv read error or not exist: %s"%e,input)

    if(len(hdfsource)==0):
        print("HDF5 source is empty")
        sys.exit(0)

    plates = np.asarray(plates)
    mjds = np.asarray(mjds)
    fibers = np.asarray(fibers)

    return (plates,mjds,fibers,hdfsource)


def list_csv(x):
    '''
       Return a array[list], where each list is a column
    Args:
        csv file
    '''
    columns = defaultdict(list) # each value in each column is appended to a list
    try:
     with open(x) as f:
      reader = csv.DictReader(f,delimiter=' ') # read rows into a dictionary format
      for row in reader: # read a row as {column1: value1, column2: value2,...}
        for (k,v) in row.items(): # go over each column name and value 
            columns[k].append(v) # append the value into the appropriate list
                                 # based on column name k
    except Exception as e:
     print ("read pmf csv error")
     traceback.print_exc()
     sys.exit()
    return columns

def _traverse_fibernode(name):
    '''
       para   : node name in a hdf5 group
       purpose: Find a dataset node, which should be an endpoint in its group hierarchy
       return : (key,value)->(path_to_dataset, (dataset type, shape, filename)) 
    '''
    global fx,pid,fiberdatalink,inputfile
    try:
     cur_node=name.encode('ascii','ignore')
     node=pid+'/'+cur_node
     node_t=str(type(fx[node]))
     if 'dataset' in node_t:
        node_t=fx[node].dtype
        node_sp=fx[node].shape
        fiberdatalink[node]=(node_t,node_sp,inputfile)
    except Exception as e:
     traceback.print_exc()
     pass
def dedup(dict1):# might be costly, as the python tuple is inmutable, 
    #Deduplication, dict(plate/mjd/../coadds, ifile, fiberlists, fiberoffsetlists)
    #TODO: Optimize this stupid: currently: pop out each key, dedup, sort, then re-insert it in the dictionary. 
    for idict in dict1:
       v1=dict1[idict][0]
       v2=dict1[idict][1]
       v3=dict1[idict][2]
       v2_len=len(v2)
       v3_len=len(v3)
       v2=list(set(v2))
       v2.sort()
       v3=list(set(v3))
       v3.sort()
       dedup_v2_len=len(v2)
       dedup_v3_len=len(v3)
       dict1.pop(idict,None)
       dict1[idict]=(v1,v2,v3)
       if v2_len!=dedup_v2_len or v3_len!=dedup_v3_len:
          print("key:%s, v2:%d,dv2:%d; v3:%d,dv3:%d"%(idict,v2_len,dedup_v2_len,v3_len,dedup_v3_len))
    return dict1
#node_type is used in ../script/subset_mpi.py, which is to create single shared file 
def get_fiberlink_v1(infile,plates,mjds,fibers):
        '''
           para  : filename, plate, mjd, fiber
           return: (key, value)->(plates/mjd/fiber/../dataset, (type,shape,filename))
           python dict's updating can ensure that the key is unique, i.e., plate/mjd/fiber/../dataset is unique
        '''
        global pid,fiberdatalink, cataloglink, fx, inputfile
        inputfile=infile
        try:
         fx = h5py.File(infile, mode='r')
         for plate in fx.keys():
            for mjd in fx[plate].keys():
                ii = (plates == plate) & (mjds == mjd)
                spid= '{}/{}'.format(plate, mjd)
                xfibers = fibers[ii]
                if np.any(ii): # fiber is found
                  for fiber in xfibers:#for each fiber node, recursively visit its members and record the 
                      #fiberlink={id:infile}
                      pid = '{}/{}/{}'.format(plate, mjd, fiber)
                      fx[pid].visit(_traverse_fibernode)
                #for im in meta:
                #  mnode=spid+'/'+im
                #  mnode_t=fx[mnode].dtype
                #  mnode_sp=fx[mnode].shape
                #  fiberdatalink[mnode]=(mnode_t,mnode_sp,infile)
         fx.close()
        except Exception as e:
         print (pid)
         traceback.print_exc()
         print (pid,infile)
         pass
        return (fiberdatalink)
def get_fiberlink_v2(infile,plates,mjds,fibers):
        '''
           para  : filename, plate, mjd, fiber
           return: fiberdatalink:
                       (key, value)->(plates/mjd/coadds,  (filename, fiberlist, fiberoffsetlist))
                       (key, value)->(plates/mjd/exposures/?id/b(r), (filename, fiberlist, fiberoffsetlist))

        '''
        #global pid,fiberdatalink, cataloglink, fx, inputfile
        #inputfile=infile
        fiberdatalink={}
        import os.path
        if not os.path.isfile(infile):
           return fiberdatalink
        try:
         fx = h5py.File(infile, mode='r')
         for plate in fx.keys():
            for mjd in fx[plate].keys():
                ii = (plates == plate) & (mjds == mjd)
                spid= '{}/{}'.format(plate, mjd)
                # get the fiber column
                #TODO: try the fx[spid+'/plugmap'][()]['FIBERID'] or fx[spid+'/plugmap'][:]['FIBERID']
                data_value=fx[spid+'/plugmap'].value['FIBERID'] 
                xfibers = fibers[ii]
                fiber_list=data_value.tolist()
                if np.any(ii): # plate and mjd are matching
                   for fiber in xfibers:
                    # return spid,
                    fiber=int(fiber)
                    if fiber not in fiber_list:
                       #print("fiber not found,e..g, %s"%(fiber_list.dtype))
                       continue
                    try: 
                     fiber_offset=fiber_list.index(fiber) # this may triger error if fiber not found
                      #update k,v store
                     #print ("fiber_offset:%d"%(fiber_offset))
                     spid_coad=spid+'/coadds'
                     if spid_coad not in fiberdatalink: # when coadds is added, exposures will be also added
                        #print ("spid_coad:%s not in fiberdata, fiber now is %d"%(spid_coad,fiber))
                        fiberlist=list()
                        fiberlist.append(fiber)
                        offsetlist=list()
                        offsetlist.append(fiber_offset)
                        #spid_coad=spid+'/coadds'
                        fiberdatalink[spid_coad]=(infile,fiberlist,offsetlist)
                        spid_expo=spid+'/exposures'
                        #try:
                        #    print("fx[%s].keys:%s"%(spid_expo,fx[spid_expo].keys()))
                        #except Exception as e:
                        #    print("get key error in infile:%s, fx[%s]"%(infile,spid_expo))
                        #    pass
                        for expid in fx[spid_expo].keys():
                            expid_name=spid_expo+'/'+expid+'/b'
                            #print("expid:%s"%expid)
                            fiberlist=list()
                            fiberlist.append(fiber)
                            offsetlist=list()
                            offsetlist.append(fiber_offset)
                            fiberdatalink[expid_name]=(infile,fiberlist,offsetlist)
                            expid_name=spid_expo+'/'+expid+'/r'
                            fiberlist=list()
                            fiberlist.append(fiber)
                            offsetlist=list()
                            offsetlist.append(fiber_offset)
                            fiberdatalink[expid_name]=(infile,fiberlist,offsetlist)
                            #print ("BEFORE0:fiberdatalink[%s][1]:%s"%(expid_name,fiberdatalink[expid_name][1]))
                     else:
                        #print ("spid_coad:%s in fiberdata, fiber now is %d"%(spid_coad,fiber))
                        #print ("before:",fiberdatalink[spid_coad]) 
                        fiberdatalink[spid_coad][1].append(fiber)  # update fiberlist
                        #print ("after:",fiberdatalink[spid_coad])
                        fiberdatalink[spid_coad][2].append(fiber_offset) # update offsetlist
                        spid_expo=spid+'/exposures'
                        for expid in fx[spid_expo].keys():
                            expid_name=spid_expo+'/'+expid+'/b'
                            expid_name_r=spid_expo+'/'+expid+'/r'
                            #print ("BEFORE1:fiberdatalink[%s][1]:%s"%(expid_name,fiberdatalink[expid_name][1]))
                            #print ("BEFORE1:fiberdatalink[%s][1]:%s"%(expid_name_r,fiberdatalink[expid_name_r][1]))
                            fiberdatalink[expid_name][1].append(fiber)
                            fiberdatalink[expid_name][2].append(fiber_offset)
                            #print ("AFTER1:fiberdatalink[%s][1]:%s"%(expid_name_r,fiberdatalink[expid_name_r][1]))
                            expid_name=spid_expo+'/'+expid+'/r'
                            #print ("BEFORE2:fiberdatalink[%s][1]:%s"%(expid_name,fiberdatalink[expid_name][1]))
                            fiberdatalink[expid_name][1].append(fiber)
                            fiberdatalink[expid_name][2].append(fiber_offset)
                            #print("expid_name:%s,fiber:%d"%(expid_name,fiber))
                            #print("fiber",fiber)
                            #print("fiber_offset",fiber_offset)
                            #print ("AFTER2:fiberdatalink[%s][1]:%s"%(expid_name,fiberdatalink[expid_name][1]))
                    except Exception as e:
                     #print("fiber kv update error:file:%s,spid:%s"%(infile,spid))
                     #traceback.print_exc() 
                     pass # fiber not existing
         fx.close()
        except Exception as e:
         #print (spid)
         traceback.print_exc()
         #print (spid,infile)
         pass
        return (fiberdatalink)
def get_catalogtypes(infile):
    '''
     para:hdf5 file
     return: dict: meta, (type, shape) 
    '''
    catalog_types={}
    try:
      fx = h5py.File(infile, mode='r')
      plate=fx.keys()[0]
      mjd=fx[plate].keys()[0]
      for im in meta:
       mnode=plate+'/'+mjd+'/'+im
       #print (mnode)
       mnode_t=fx[mnode].dtype
       mnode_sp=fx[mnode].shape
       #print (mnode_t)
       catalog_types[im]=(mnode_t,mnode_sp)
    except Exception as e:
       print ("file:",infile)
       traceback.print_exc()
       pass
    return catalog_types
def count_unique(global_dict):
     '''
      para: dict: (plates/mjd/fiber/../dataset, (type,shape,filename))
      return: dict: (plates/mjd, num_fibers)
     '''
     count_fiber={}
     for key, value in global_dict.items():
         dname=key.split('/')[-1]
         if dname=='coadd':
          plate=key.split('/')[0]
          mjd=key.split('/')[1]
          temp_key=plate+'/'+mjd
          #print (temp_key)
          if temp_key in count_fiber: 
             pre_count=int(count_fiber[temp_key])
             count_fiber[temp_key]=pre_count+1
          else: 
             count_fiber[temp_key]=int(1)
     return count_fiber
def fiber_union(fiber_dict1, fiber_dict2,interkey_set):
    if len(interkey_set)==0: 
       return fiber_dict1
    for ikey in interkey_set:
       fiber_dict1[ikey][1]+=fiber_dict2[ikey][1]
       fiber_dict1[ikey][2]+=fiber_dict2[ikey][2]
       #TODO: need to remove duplication,Sep 23. 2016
    return fiber_dict1
def locate_fiber_in_catalog(global_dict):
    revised_dict={}# key: pm, value: (fiberid, global_offset, infile)
    for key,value in global_dict.items():
        if key.split('/')[-1]=='coadd':
            pm=key.split('/')[0]+'/'+key.split('/')[1]
            if pm in revised_dict.keys():
                latest=len(revised_dict[pm])
                new_value=(key.split('/')[2],value[2],latest)
                revised_dict[pm].append(new_value)
            else:
                latest=0
                new_value=(key.split('/')[2],value[2],latest)
                revised_dict.setdefault(pm, [])
                revised_dict[pm].append(new_value)
    return revised_dict

