import h5py
import sys
fx=h5py.File(sys.argv[1],'a')
fx.close()
