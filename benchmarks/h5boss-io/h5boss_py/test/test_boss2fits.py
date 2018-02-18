#!/usr/bin/env python

"""
Reformat spectra into a single fits file per object, combining all necessary
pieces from spPlate, spCFrame, spFrame, spFlat, and spZbest

Stephen Bailey, Summer 2011

Bugs/Features:
  - Assumes RUN1D=RUN2D, and gets them from spAll (not env vars), assuming
    that spAll has one and only one RUN2D.
"""

import sys
import os
import os.path
from glob import glob           #- File pattern globbing (spFrame*.fits)
import re                       #- Regular expressions
from time import asctime
import numpy as np
from astropy.io import fits

class CFrame(object):
    """
    Convenience wrapper class for spCFrame, spFrame, and spFlat data
    
    Derives spFrame dir/name from spCFrame, and assumes that it is gzipped.
    """
    def __init__(self, cframefile):

        #- Load original framefile and find out original dimensions
        ### print cframefile
        framefile = cframefile.replace('spCFrame', 'spFrame') + '.gz'
        eflux = fits.getdata(framefile, 0)
        nfiber, npix = eflux.shape
                
        #- Load spCFrame file; trim arrays back to original size
        fx = fits.open(cframefile)
        self.flux = fx[0].data[:, 0:npix]
        self.ivar = fx[1].data[:, 0:npix]
        self.mask = fx[2].data[:, 0:npix]
        self.loglam = fx[3].data[:, 0:npix]
        self.wdisp  = fx[4].data[:, 0:npix]
        self.sky    = fx[6].data[:, 0:npix]
        self.x      = fx[7].data[:, 0:npix]
        self.header = fx[0].header
        
        #- Load superflat spCFrame[8] and fiberflat spFlat[0]
        filedir, basename = os.path.split(cframefile)
        superflat = fx[8].data[:, 0:npix]
        flatfile = fx[0].header['FLATFILE'].replace('sdR', 'spFlat')
        flatfile = flatfile.replace('.fit', '.fits.gz')
        flatfile = os.path.join(filedir, flatfile)
        fiberflat = fits.getdata(flatfile, 0)
        
        #- Calculate calibration vector: flux = electrons * calib
        electrons = eflux * fiberflat * superflat
        ii = np.where(electrons != 0.0)
        self.calib = np.zeros(self.flux.shape)
        self.calib[ii] = self.flux[ii] / electrons[ii]
                
        fx.close()

def load_spCFrame_files(platedir):
    """
    Load all spCFrame files in a given directory.
    Return a dictionary of CFrame objects, keyed by camera-expid string
    """
    print "loading spCFrame files from " + platedir
    cframes = dict()
    for filename in glob(os.path.join(platedir, 'spCFrame-*.fits')):
        print '   ', os.path.basename(filename), asctime()
        expid = get_expid(filename)
        cframes[expid] = CFrame(filename)

    return cframes

def good_ivar(ivar, fiber=None):
    """
    return indices of for array from first non-zero ivar to the last
    non-zero ivar.  i.e. trim off leading and trailing contiguously zero ivars.
    
    If all ivar==0, return indices for full array since code might get
    grumpy with completely blank arrays.
    """
    if np.all(ivar == 0):
        if fiber is not None:
            print 'WARNING: All ivar==0 for fiber', fiber
        else:
            print 'WARNING: All ivar==0'
        return np.arange(len(ivar))

    ivar_good = np.where(ivar > 0.0)[0]
    return np.arange(ivar_good[0], ivar_good[-1]+1)

def get_expid(filename):
     """parse /path/to/spBlat-b1-00123456.fits.gz into b1-00123456"""
     try:
         return re.search('-([br][12]-\d{8}).fits', filename).group(1)
     except AttributeError:  #- search failed
         return None
        
def process_plate(indir, outdir, plate, mjd):
    """
    Process a plate's worth of objects
    
    Inputs
        datadir : input base directory, e.g. $BOSS_SPECTRO_REDUX/v5_4_40/
        outdir  : output base directory
        plate   : plate to process
        mjd     : mjd to process
        
    Outputs:
        writes files to outdir/plate/spec-plate-mjd-fiber.fits
    """
    #- Load all C/Frame files for this plate
    cframes = load_spCFrame_files(indir)

    #- Open spPlate, spZbest, and spZline files
    spPlateFile = '%s/spPlate-%d-%d.fits' % (indir, plate, mjd)
    print 'Processing', os.path.basename(spPlateFile)
    FXplate = fits.open(spPlateFile, memmap=True)

    #- Remove spurious EXPID** if needed
    if 'EXPID**' in FXplate[0].header:
        FXplate[0].header.remove('EXPID**')

    code_version = FXplate[0].header['RUN2D']

    spZbestFile = '%s/%s/spZbest-%d-%d.fits' % \
        (indir, code_version, plate, mjd)
    FXzbest = fits.open(spZbestFile, memmap=True)
    
    spZlineFile = '%s/%s/spZline-%d-%d.fits' % \
        (indir, code_version, plate, mjd)
    zline = fits.getdata(spZlineFile, 1)

    #- HDU0 will be a modified copy of the spPlate header
    plate_hdu = fits.PrimaryHDU(header=FXplate[0].header)
    
    #- Loop over fibers on this plate on this MJD
    for fiber in range(1000):
        #- HDU1 : binary table of coadd flux, log(lambda), ivar, etc.
        flux = FXplate[0].data[fiber]
        c0   = FXplate[0].header['COEFF0']
        c1   = FXplate[0].header['COEFF1']
        loglam = c0 + c1*np.arange(len(flux))
        ivar     = FXplate[1].data[fiber]
        and_mask = FXplate[2].data[fiber]
        or_mask  = FXplate[3].data[fiber]
        wdisp    = FXplate[4].data[fiber]
        sky      = FXplate[6].data[fiber]
        model    = FXzbest[2].data[fiber]

        #- trim off leading and trailing ivar=0 bins,
        #- but keep ivar=0 bins in the middle of the spectrum
        igood = good_ivar(ivar, fiber=fiber)
        new_coeff0 = round(float(loglam[igood[0]]),4) #- fix float32 rounding

        #- Create coadded spectrum table for HDU 1
        cols = list()
        cols.append( fits.Column(name='flux',     format='E', array=flux[igood]) )
        cols.append( fits.Column(name='loglam',   format='E', array=loglam[igood]) )
        cols.append( fits.Column(name='ivar',     format='E', array=ivar[igood]) )
        cols.append( fits.Column(name='and_mask', format='J', array=and_mask[igood]) )
        cols.append( fits.Column(name='or_mask',  format='J', array=or_mask[igood]) )
        cols.append( fits.Column(name='wdisp',    format='E', array=wdisp[igood]) )
        cols.append( fits.Column(name='sky',      format='E', array=sky[igood]) )
        cols.append( fits.Column(name='model',    format='E', array=model[igood]) )
        
        cols = fits.ColDefs(cols)
        coadd_hdu = fits.BinTableHDU.from_columns(cols)
        hdux = [plate_hdu, coadd_hdu]
        
        #- HDU 2: copy of rows from spZline
        ii = np.where(zline.FIBERID == fiber)[0]
        hdux.append( fits.BinTableHDU(data=zline[ii]) )
        
        #- HDU 3 .. 3+n : spectra from individual exposures
        #- Loop over individual exposures.  Do this even if we aren't
        #- writing those HDUs, so that we can update the headers with
        #- which exposures went into the coadd
        nexp = 0
        fullexpids = list()
        for iexp in range(1, 100):
            key = 'EXPID%02d' % iexp
            if key not in FXplate[0].header:
                break

            expid = FXplate[0].header[key][0:11]  #- e.g. b1-00123456

            #- check camera for this fiber
            camera = expid[0:2]
            if fiber <= 500 and camera in ('b2', 'r2'):
                continue
            elif fiber > 500 and camera in ('b1', 'r1'):
                continue
                
            #- If we got this far, we're going to use this exposure
            fullexpids.append(FXplate[0].header[key])
            nexp += 1
        
            nfiber = cframes[expid].header['NAXIS2']
            ifib = (fiber) % nfiber
            d = cframes[expid]

            #- trim off leading and trailing ivar=0 bins,
            #- but keep ivar=0 bins in the middle of the spectrum
            igood = good_ivar(d.ivar[ifib], fiber='%s %d' % (expid, fiber))
    
            cols = list()
            cols.append( fits.Column(name='flux',   format='E', array=d.flux[ifib][igood]) )
            cols.append( fits.Column(name='loglam', format='E', array=d.loglam[ifib][igood]) )
            cols.append( fits.Column(name='ivar',   format='E', array=d.ivar[ifib][igood]) )
            cols.append( fits.Column(name='mask',   format='J', array=d.mask[ifib][igood]) )
            cols.append( fits.Column(name='wdisp',  format='E', array=d.wdisp[ifib][igood]) )
            cols.append( fits.Column(name='sky',    format='E', array=d.sky[ifib][igood]) )
            cols.append( fits.Column(name='calib',  format='E', array=d.calib[ifib][igood]) )
            cols.append( fits.Column(name='x',      format='E', array=d.x[ifib][igood]) )

            #- Place holder - someday we may want to calculate and include
            #- the "extra" variance which isn't proportional to the signal.
            ### n = len(d.flux[ifib])
            ### cols.append( fits.Column(name='var_extra',  format='E', array=np.zeros(n) ) )
    
            cols = fits.ColDefs(cols)
            hdux.append( fits.new_table(cols, header=d.header) )

        #- Convert to pyfits HDUList
        hdux = fits.HDUList( hdux )
            
        #- Change some keyword headers which don't make sense when
        #- converting a plate header into a single object header

        #- HDU 0 is now a blank image, so fitsverify doesn't like CRPIX1 etc.
        hdr = hdux[0].header
        del hdr['CRPIX1']
        del hdr['CRVAL1']
        del hdr['CTYPE1']
        del hdr['CD1_1']

        #- We trimmed leading/trailing ivar=0 pixels, so update COEFF0
        hdr.update('COEFF0', new_coeff0)

        #- Remove original expid list which has both SP1 and SP2
        nexp_orig = hdr['NEXP']
        del hdr['NEXP']
        for iexp in range(nexp_orig):
            expid = "EXPID%02d" % (iexp+1, )
            del hdr[expid]
            
        #- Add new NEXP, EXPID list for just the exposures in this file
        #- Update EXTNAME of individual exposure HDUs with this expid
        hdr.update('NEXP', nexp, 'Number of individual exposures')
        for iexp, expid in enumerate(fullexpids):
            key = "EXPID%02d" % (iexp+1, )
            hdr.update(key, expid)
            ### print "Setting EXTNAME for %d to %s" % (4+iexp, expid)
            hdux[3+iexp].update_ext_name(expid)

        #- Remove mention of the other spectrograph
        #- sp1
        if fiber <= 500:            #- sp1
            del hdr['NEXP_B2']
            del hdr['NEXP_R2']
            del hdr['EXPT_B2']
            del hdr['EXPT_R2']                
        else:                       #- sp2
            del hdr['NEXP_B1']
            del hdr['NEXP_R1']
            del hdr['EXPT_B1']
            del hdr['EXPT_R1']

        #- Delete a bunch of per-exposure keywords which came along for
        #- the ride in the spPlate header
        for keyword in """
        NGUIDE 
        SEEING20 SEEING50 SEEING80
        RMSOFF20 RMSOFF50 RMSOFF80 AZ       ALT      AIRMASS
        DAQVER   CAMDAQ   SUBFRAME ERRCNT   SYNCERR  SLINES
        PIXERR   PLINES   PFERR    DIDFLUSH TAI-BEG  TAI-END
        DATE-OBS OBJSYS   ROTTYPE  ROTPOS   BOREOFF  ARCOFFX  ARCOFFY
        OBJOFFX  OBJOFFY  CALOFFX  CALOFFY  CALOFFR  GUIDOFFX GUIDEOFFY
        GUIDOFFR FOCUS
        M2PISTON M2XTILT  M2YTILT  M2XTRAN  M2YTRAN
        M1PISTON M1XTILT  M1YTILT  M1XTRAN  M1YTRAN
        SCALE    POINTING GUIDER1  SLITID1  SLIDID2  GUIDERN
        COLLA    COLLB    COLLC
        HARTMANN MC1HUMHT MC1HUMCO MC1TEMDN MC1THT   MC1TRCB  MC1TRCT
        MC1TBCB  MC1TBCT  AUTHOR   TWOPHASE XSIGMA   XSIGMIN  XSIGMAX
        WSIGMA   WSIGMIN  WSIGMAX  LAMPLIST SKYLIST  UNAME
        """.split():
            del hdr[keyword]

        #- Add some additional header keywords
        # hdr.update('PLUG_RA',  spAll.PLUG_RA[ispec],  'RA of object [deg]')
        # hdr.update('PLUG_DEC', spAll.PLUG_DEC[ispec], 'dec of object [deg]')
        # hdr.update('THING_ID', spAll.THING_ID[ispec], 'Unique object identifier')
        # hdr.update('FIBERID',  spAll.FIBERID[ispec],  'Fiber number (1-1000)')

        #- Update other headers with useful comments
        hdux[1].header.add_comment('Coadded spectrum')
        hdux[1].update_ext_name('COADD')
        hdux[2].header.add_comment('Line fits from spZline')
        hdux[2].update_ext_name('SPZLINE')

        #- BUNIT is invalid for binary table HDUs
        for i in range(1, len(hdux)):
            if 'BUNIT' in hdux[i].header:
                del hdux[i].header['BUNIT']

        #- Write final file
        outfile = '%s/spec-%d-%d-%04d.fits' % (outdir, plate, mjd, fiber)
        ### print mjd, os.path.basename(outfile)
        try:
            hdux.writeto(outfile, clobber=True, output_verify='fix')
        except fits.core.VerifyError, err:
            print "Unable to write %s" % outfile
            raise err
        
    #- Done with this plate-mjd; close input files
    FXplate.close()
    FXzbest.close()

def write_file_list(filename, spectra):
    FX = open(filename, 'w')
    for plate, mjd, fiber in sorted( zip(spectra.PLATE, spectra.MJD, spectra.FIBERID) ):
        specfile = "%04d/spec-%04d-%05d-%04d.fits" % (plate, plate, mjd, fiber)
        print >> FX, specfile

    FX.close()

def parse_string_range(s):
    """
    e.g. "1,2,5-8,20" -> [1,2,5,6,7,8,20]

    modified from Sven Marnach,
    http://stackoverflow.com/questions/5704931/parse-string-of-integer-sets-with-intervals-to-list

    Feature/Bug: Only works with positive numbers
    """
    ranges = (x.split("-") for x in s.split(","))
    x = [i for r in ranges for i in range(int(r[0]), int(r[-1]) + 1)]
    return x
    
#-----------------------------------------------------------------------------
#- Parse command line options and call subroutines

import optparse

parser = optparse.OptionParser(usage = "%prog [options]")
parser.add_option("-i", "--indir",  type="string",  help="input directory [$BOSS_SPECTRO_REDUX/$RUN2D/]")
parser.add_option("-o", "--outdir", type="string",  help="output directory")
opts, args = parser.parse_args()

if not os.path.isdir(opts.outdir):
    os.makedirs(opts.outdir)

#- use spPlate files to determine (plate, mjd) combinations
spPlateFiles = glob(opts.indir+'/spPlate*.fits')
for filename in spPlateFiles:
    plate, mjd = map(int, re.search('spPlate\-(\d+)\-(\d+)', filename).groups())
    process_plate(opts.indir, opts.outdir, plate, mjd)
            
print "Wrote files to " + opts.outdir
print "Done", asctime()
            
        
