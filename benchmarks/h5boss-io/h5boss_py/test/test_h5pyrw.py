"""
Create an HDF5 file from BOSS data

TODO:
  - include comments in meta/attrs
  - platelist quantities
"""
from __future__ import division, print_function
import unittest
import sys,os
import time
import optparse
import csv
import traceback
fx=h5py.File('$SCRATCH/h5boss/3701-55540.hdf5','r')
hx=h5py.File('check.h5','a')
dx=fx['3701/55540/1/coadds/flux'].value
