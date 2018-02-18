import h5py 
import cPickle as pickle
from mpi4py import MPI
import argparse
import traceback
import os
catalog_meta=['plugmap', 'zbest', 'zline',
                        'match', 'matchflux', 'matchpos']
#ver latest, earliest
def _fiber_template(hx,key,value,allotime):
   space=h5py.h5s.create_simple(value[1])
   plist=h5py.h5p.create(h5py.h5p.DATASET_CREATE)
   if allotime=="early":
    plist.set_alloc_time(h5py.h5d.ALLOC_TIME_EARLY)
   elif allotime=="late":
    plist.set_alloc_time(h5py.h5d.ALLOC_TIME_LATE)
   else:
    print ("wrong allo time")
   tid=h5py.h5t.py_create(value[0], False)
   try:#create intermediate groups
      hx.create_group(os.path.dirname(key))
   except Exception as e:
      #traceback.print_exc() #groups existed, so pass it
      pass
   try:
    h5py.h5d.create(hx.id,key,tid,space,plist)
   except Exception as e:
    #traceback.print_exc()
    pass
def create_template(outfile, global_dict,ver, allotime,idrive):
 try:
     if idrive=='None':
      hx = h5py.File(outfile,'a',libver=ver)
     else:
      hx = h5py.File(outfile,'a',libver=ver,driver=idrive)
 except Exception as e:
     traceback.print_exc()
 try: 
  for key,value in global_dict.items():
    if key.split('/')[-1] not in catalog_meta:
     _fiber_template(hx,key,value, allotime)
 except Exception as e:
   traceback.print_exc()
   pass
 try:
  hx.flush()
  hx.close()
 except Exception as e:
  print("hx close error in")
  traceback.print_exc()

def test_create():
    parser = argparse.ArgumentParser(prog='template_create')
    parser.add_argument("input",  help="pickle input")
    parser.add_argument("output", help="HDF5 output")
    parser.add_argument("version", help="HDF5 version, ear")    
    parser.add_argument("allocate", help="Allocate time")
    parser.add_argument("driver", help="IO driver")

    opts=parser.parse_args()

    global_fiber = opts.input
    outfile = opts.output
    version = opts.version
    allotime = opts.allocate
    idrive = opts.driver
    comm=MPI.COMM_WORLD
    nproc = comm.Get_size()
    rank = comm.Get_rank()
    try:
     globalfiber=pickle.load(open(global_fiber,"rb"))
     print ("number of objects:%d"%(len(globalfiber)))
    except Exception as e:
     traceback.print_exc()
    t1=MPI.Wtime()
    if rank==0:
       try:
         create_template(outfile,globalfiber,version,allotime,idrive)
       except Exception as e:
         traceback.print_exc()
    t2=MPI.Wtime()
    if rank==0:
       print("template creation cost %.2f"%(t2-t1))
if __name__=='__main__':
    test_create()
