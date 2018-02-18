import numpy as np
import h5py
import time
import sys
import os
import csv
import traceback
import os.path
import gzip
import cPickle as pickle 
from collections import defaultdict
fx=""
pid=""
inputfile=""
fiberdatalink={}
cataloglink={}
meta=['plugmap', 'zbest', 'zline',
                        'photo/match', 'photo/matchflux', 'photo/matchpos']

def _traverse_fibernode(name):
    '''
       para   : node name in a hdf5 group
       purpose: Find a dataset node, which should be an endpoint in its group hierarchy
unique_datsetpath datasettype datasetshape filepath plate mjd fiber
       return : (key,value)->(path_to_dataset, (dataset type, shape, filename, plate, mjd, fiber)) 
    '''
    global fx,pid,fiberdatalink,inputfile
    try:
     cur_node=name.encode('ascii','ignore')
     node=pid+'/'+cur_node
     p=pid.split('/')[0]
     m=pid.split('/')[1]
     f=pid.split('/')[2]
     node_t=str(type(fx[node]))
     if 'dataset' in node_t:
        node_t=fx[node].dtype
        node_sp=fx[node].shape
        fiberdatalink[node]=(node_t,node_sp,inputfile,p,m,f)
    except Exception as e:
     traceback.print_exc()
     pass
#node_type is used in ../script/subset_mpi.py, which is to create single shared file 
def map_fiber(infile):
        '''
           para  : filename
           return: (key, value)->(plate/mjd/fiber/../dataname, (dtype, shape, filename, plate, mjd, fiber))
           python dict's updating can ensure that the key is unique, i.e., datapath: plate/mjd/fiber/../dataset is unique
        '''
        global pid,fiberdatalink, cataloglink, fx, inputfile
        inputfile=infile
        try:
         fx = h5py.File(infile, mode='r')
         for plate in fx.keys():
            for mjd in fx[plate].keys():
               spid= '{}/{}'.format(plate, mjd)
               for fib in fx[spid].keys():
                   if (fib.isdigit()):
                    pid = '{}/{}/{}'.format(plate, mjd, fib)
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
def _traverse_fibernode_simple(name):
    '''
       para   : node name in a hdf5 group
       purpose: Find a dataset node, which should be an endpoint in its group hierarchy
unique_datsetpath datasettype datasetshape filepath plate mjd fiber
       return : (key,value)->(path_to_dataset, (shape)) 
    '''
    global fx,pid,fiberdatalink,inputfile
    try:
     cur_node=name.encode('ascii','ignore')
     node=pid+'/'+cur_node
     p=pid.split('/')[0]
     m=pid.split('/')[1]
     f=pid.split('/')[2]
     node_t=str(type(fx[node]))
     if 'dataset' in node_t:
        node_sp=fx[node].shape[0]
        fiberdatalink[node]=(node_sp)
    except Exception as e:
     #traceback.print_exc()
     pass
def map_fiber_simple(infile):
        '''
           para  : filename
           return: (key, value)->(plate/mjd/fiber/../dataname, (dtype, shape, filename, plate, mjd, fiber))
           python dict's updating can ensure that the key is unique, i.e., datapath: plate/mjd/fiber/../dataset is unique
        '''
        global pid,fiberdatalink, cataloglink, fx, inputfile
        inputfile=infile
        fiberdatalinkt={}
        try:
         fx = h5py.File(infile, mode='r')
         for plate in fx.keys():
            for mjd in fx[plate].keys():
               spid= '{}/{}'.format(plate, mjd)
               for fib in fx[spid].keys():
                   if (fib.isdigit()):
                    pid = '{}/{}/{}'.format(plate, mjd, fib)
                    fx[pid].visit(_traverse_fibernode_simple)
                #for im in meta:
                #  mnode=spid+'/'+im
                #  mnode_t=fx[mnode].dtype
                #  mnode_sp=fx[mnode].shape
                #  fiberdatalink[mnode]=(mnode_t,mnode_sp,infile)
               # add type infor for coadd/exposures, fname
               try:
                k_coadd='{}/{}/coadd'.format(plate,mjd)
                v_coadd=fx[spid+'/1/coadd'].dtype
                eid=fx[spid+'/1/exposures/'].keys()[0]
                k_exposure='{}/{}/exposures'.format(plate,mjd)
                v_exposure=fx[spid+'/1/exposures/'+eid+'/b'].dtype
                k_file='{}/{}/filename'.format(plate,mjd)
                v_file=infile
                fiberdatalinkt[k_coadd] = v_coadd
	        fiberdatalinkt[k_exposure] = v_exposure
                fiberdatalinkt[k_file] = v_file
               except Exception as e:
                pass
         fx.close()
        except Exception as e:
         print (pid)
         #traceback.print_exc()
         print (pid,infile)
         pass
        return (fiberdatalink,fiberdatalinkt)

def map_pmf(infile):
        '''
           para  : filename
           return: (key, value)->(plates/mjd/fiber, filename)
           python dict's updating can ensure that the key is unique, i.e., plate/mjd/fiber/../dataset is unique
        '''
        pmf={}
        try:
         fx = h5py.File(infile, mode='r')
         for plate in fx.keys():
            for mjd in fx[plate].keys():
               spid= '{}/{}'.format(plate, mjd)
               for fib in fx[spid].keys():
                   if fib.isdigit():
                    pid = '{}/{}/{}'.format(plate, mjd, fib)
                    pmf[pid]=infile 
         fx.close()
        except Exception as e:
         print (pid)
         traceback.print_exc()
         print (pid,infile)
         pass
        return (pmf)
def type_map(infile):
    '''
       para  : filename
       return: type and shape for each object, returned as tuple, dmap[0] is coadds, dmap[1] is exposure
       plate/mjd/coadds: 8 datasets
       plate/mjd/exposure/exposureid/b(r): 8 datasets
       # (key, value)->(plates/mjd/, (filename, fiberlist, fiberoffsetlist))   
    '''
    coadds_map={}
    exposures_map={}
    with h5py.File(infile,'r') as fx:
        try:
            p=fx.keys()[0]
            m=fx[p].keys()[0]
            pm=p+'/'+m
            coad_name=pm+'/coadds'
            expo_name=pm+'/exposures'
            coad=fx[coad_name].keys()
            subexpo_name=expo_name+'/'+fx[expo_name].keys()[0]+'/b'
            expo=fx[subexpo_name].keys()
            for icoad in coad:
                try:
                    icoad_name=coad_name+'/'+icoad
                    coadds_map[icoad]=(fx[icoad_name].dtype,fx[icoad_name].shape)
                except Exception as e:
                    print (icoad)
            for iexpo in expo:
                try:
                    iexpo_name=subexpo_name+'/'+iexpo
                    exposures_map[iexpo]=(fx[iexpo_name].dtype,fx[iexpo_name].shape)
                except Exception as e:
                    print (iexpo)
        except Exception as e:
            print (infile)
            traceback.print_exc()
    dmap=(coadds_map,exposures_map)
    return dmap

def coadd_map(fname_list):
    coadmap={}
    for ifile in fname_list:
     try:
      f=h5py.File(ifile,'r')
      p=f.keys()[0]
      m=f[p].keys()[0]
      pm=p+'/'+m
      pmc=pm+'/coadds'
      dsets=f[pmc].keys()
      if dsets[0]!='wave':
       dsize=f[pmc+'/'+dsets[0]].shape[1]
      else:
       dsize=f[pmc+'/'+dsets[1]].shape[0]
      coadmap[pm]=dsize
     except Exception as e: 
       pass
    return coadmap


def query_datamap(datamapfile,plates,mjds,fibers):
    '''
      This function will take the pickle file 'datamap', and run the query, 'plates, mjds, fibers', return the same structure with the global_fiber: <key,value>=<path_to_dataset,type, shape, filename>, e.g., <3973/55323/790/coadd, dtype..., 4012,/global/cscratch1/sd/jialin/h5boss/3973-55323.hdf5>
      para: query tuple: plates,mjds,fibers, 
      para: pickle file: datamap
      return: global_fiber
    ''' 
    # reading pickle zipped file: datamap
    assert(os.path.isfile(datamapfile))
    try:
        fp=gzip.open(datamapfile,'rb') 
        datamap=pickle.load(fp)
    except Exception as e:
        print ("loading datamap pickle file:%s error"%datamapfile)
    fiber_dict={}
    lenp=len(plates)
    assert(lenp==len(mjds))
    assert(lenp==len(fibers)) 
    assert(lenp>0)
    # for each pmf, return the list of fiber_dict
    for i in range(0,lenp): 
       pmf=(plates[i],mjds[i],fibers[i])
       keysall=_get_allkey(pmf)
       fiber_item={}
       try:
        fiber_item=_query_datamap(keysall,datamap,pmf)
       except Exception as e:
        print ("pmf not found in existing datamap:",pmf)
        pass
       if (len(fiber_item)>0):
         fiber_dict.update(fiber_item)  

    return fiber_dict
def fuzzy_search(fuzzy_key,datamap):
    result = [key for key in datamap if fuzzy_key in key.lower()]
    return result

def _get_allkey(pmf):
    plate=pmf[0]
    mjd=pmf[1]
    fiber=pmf[2]
    keysall=list()
    keycoaddt='{}/{}/coadd'.format(plate,mjd) # this will return the type of coadd in plate/mjd
    keyexpost='{}/{}/exposures'.format(plate,mjd) # this will return the type of exposure in plate/mjd 
    keyfilename='{}/{}/filename'.format(plate,mjd) # filename of plate/mjd
    keycoadds='{}/{}/{}/coadd'.format(plate,mjd,fiber) # datashape of fiber coadd
    keyexposs_fuzzy='{}/{}/{}/exposures'.format(plate,mjd,fiber) # datashape of exposures  
    keysall.append(keycoaddt)
    keysall.append(keyexpost)
    keysall.append(keycoadds)
    keysall.append(keyexposs_fuzzy)
    keysall.append(keyfilename)
    return keysall
def _query_datamap(keysall,datamap,pmf):
    #print ("Length of datamap:%d"%len(datamap))
    #print ("Query: ",pmf)
    #print ("Known key: ",keysall)
    coaddtype=datamap[keysall[0]] # coadd type
    expostype=datamap[keysall[1]] # exposure type
    coaddshape=datamap[keysall[2]] # coadd shape
    #exposure fuzzy generator
    exposure_keylist=fuzzy_search(keysall[3],datamap)
    filename=datamap[keysall[4]]  # file name
    # fill into the dict list
    # fill coadd first
    fiber_dict={} # initialize the dictionary 
    fiber_dict[keysall[2]]=(coaddtype,coaddshape,filename)
    # then for each expid, fill in the dictionary
    for iexp in exposure_keylist:
        #type for iexp is known and same for all: expostype
        #filename is same: filename
        #get the shape of each iexp
        iexposshape=datamap[iexp]
        #fill in the dic
        fiber_dict[iexp]=(expostype,(iexposshape,),filename)
    return fiber_dict     
