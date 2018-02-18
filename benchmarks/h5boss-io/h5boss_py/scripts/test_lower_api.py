import numpy as np
import h5py
import traceback
try:
 hx = h5py.File('tlow.h5','w')
except Exception as e:
 print ("Output file creat error:%s"%outfile)
 traceback.print_exc()
try:
   space=h5py.h5s.create_simple((100,))
   plist=h5py.h5p.create(h5py.h5p.DATASET_CREATE)
   plist.set_alloc_time(h5py.h5d.ALLOC_TIME_EARLY)
   tid=h5py.h5t.py_create(np.int32, False)
   hx.create_group('tyr/2/3')
   #h5py.h5g.create(hx.id, 'tyr/2')
   #h5py.h5g.create(hx.id, 'tyr/2/3')
   h5py.h5d.create(hx.id,'tyr/2/3/5t',tid,space,plist)
   hx.close()
except Exception as e:
  traceback.print_exc()
  pass
