#!/usr/bin/env python

"""
Create an HDF5 file from BOSS data

TODO:
  - include comments in meta/attrs
  - platelist quantities
"""

from __future__ import division, print_function

import sys, os
import numpy as np
from astropy.io import fits
from astropy.table import Table
import io
import time
import optparse

parser = optparse.OptionParser(usage = "%prog [options]")
parser.add_option("-i", "--input", type=str,  help="input spPlate file")
parser.add_option("-o", "--output", type=str,  help="input hdf5 file")
parser.add_option("--test", action="store_true", help="write subset for faster testing")

opts, args = parser.parse_args()
if len(sys.argv)<=1:
   print ("boss2hdf5 -help")
   exit(1)

platefile = opts.input
filedir = os.path.split(os.path.abspath(platefile))[0]

hdr = fits.getheader(platefile)
plate = hdr['PLATEID']
mjd = hdr['MJD']
tstart=time.time()
#--- Plugmap ---
print('plugmap')
plugmap = Table.read(platefile, 5)
dataname = '{}/{}/plugmap'.format(plate, mjd)
plugmap.write(opts.output, path=dataname, append=True)

#--- zbest ---
print('zbest')
run1d = hdr['RUN2D']  #- default run1d == run2d
zbestfile = platefile.replace('spPlate', '{}/spZbest'.format(run1d))
zbest = Table.read(zbestfile, 1)
dataname = '{}/{}/zbest'.format(plate, mjd)
zbest.write(opts.output, path=dataname, append=True)
nfiber = len(zbest)

#--- zall (skip) ---
pass

#--- zline ---
print('zline')
zlinefile = zbestfile.replace('spZbest-', 'spZline-')
zline = Table.read(zlinefile, 1)
dataname = '{}/{}/zline'.format(plate, mjd)
zline.write(opts.output, path=dataname, append=True)

#--- photometric matches ---
print('photo')
photomatchfile = platefile.replace('spPlate-', 'photoMatchPlate-')
photomatch = Table.read(photomatchfile, 1)
photomatch['FIBERID'] = np.arange(1, nfiber+1, dtype=np.int16)
dataname = '{}/{}/photo/match'.format(plate, mjd)
photomatch.write(opts.output, path=dataname, append=True)

photoposfile = platefile.replace('spPlate-', 'photoPosPlate-')
photopos = Table.read(photoposfile, 1)
photopos['FIBERID'] = np.arange(1, nfiber+1, dtype=np.int16)
dataname = '{}/{}/photo/matchpos'.format(plate, mjd)
photopos.write(opts.output, path=dataname, append=True)

photofluxfile = platefile.replace('spPlate-', 'photoPlate-')
photoflux = Table.read(photofluxfile, 1)
photoflux['FIBERID'] = np.arange(1, nfiber+1, dtype=np.int16)
dataname = '{}/{}/photo/matchflux'.format(plate, mjd)
photoflux.write(opts.output, path=dataname, append=True)

#--- Coadd ---
print('loading coadds')
coadds = io.load_coadds(platefile)

print('writing coadds')
for i, cx in enumerate(coadds):
    if opts.test and i%100 != 0: continue  #- TESTING            
    dataname = '{}/{}/{}/coadd'.format(plate, mjd, i+1)
    cx.write(opts.output, path=dataname, append=True)

#--- Individual exposures ---
#- Parse spPlancomb to get exposures that were used
print('parsing planfile')
planfile = platefile.replace('spPlate-', 'spPlancomb-').replace('.fits', '.par')
framefiles = list()
for line in open(planfile):
    if line.startswith('SPEXP '):
        tmp = line.split()
        tmp = [x+'.gz' for x in tmp[7:-1]]
        framefiles.extend(tmp)
        
print('individual exposures')
for filename in framefiles:
    #print(filename)
    frame = io.load_frame(filedir+'/'+filename)
    if ('spFrame-b1' in filename) or ('spFrame-r1' in filename):
        offset = 0
    elif ('spFrame-b2' in filename) or ('spFrame-r2' in filename):
        offset = 500
    else:
        print('huh?', filename)
        sys.exit(1)
    
    for i, fx in enumerate(frame):
        if opts.test and i%100 != 0: continue
        br = fx.meta['CAMERAS'][0]
        expid = fx.meta['EXPOSURE']
        fiber = offset+i+1
        dataname = '{}/{}/{}/exposures/{}/{}'.format(plate, mjd, fiber, expid, br)
        fx.write(opts.output, path=dataname, append=True)
    

tend=time.time()-tstart
print ('time:',tend)
