import h5py
from mpi4py import MPI
import traceback
out='test_pw.h5'
import numpy as np

def concurrent_write():
 comm =MPI.COMM_WORLD
 nproc = comm.Get_size()
 rank = comm.Get_rank()
 try:
   hx = h5py.File(out,'w',driver='mpio', comm=MPI.COMM_WORLD)
 except Exception as e:
  print ("Output file creat error:%s"%out)
  traceback.print_exc()
 comm.Barrier()
 temp=np.arange(10)
 print ("nproc:%d"%nproc)
 try:
  #if rank==1:
    #print ("rank:%d creating dataset d2"%rank)
    dset1=hx.create_dataset('d2',dtype=int, data=temp)
  #if rank==0:
    #print ("rank:%d creating dataset d1"%rank)
    dset2=hx.create_dataset('d1',dtype=int,data=temp)
 except Exception as e:
  print ("rank: %d, create dataset error"%rank)
  traceback.print_exc()
 try:
  #dset1.close()
  #dset2.close()
  hx.close()
 except Exception as e:
  traceback.print_exc()

if __name__=='__main__':
 concurrent_write()
