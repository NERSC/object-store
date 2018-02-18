import traceback
import os.path
import numpy as np
from astropy.io import fits
from astropy.table import Table
import h5py
expdat=("wave","flux","ivar","mask","wavedisp","sky","x","calib") #exposures
dat=("wave","flux","ivar","and_mask","or_mask","wavedisp","sky","model") # wavelength
def load_coadds(platefile, zbestfile=None, run1d=None):
    '''
    Document ...
    '''
    #- Load spPlate data
    fx = fits.open(platefile, memmap=False)
    header   = fx[0].header
    c0       = header['COEFF0']
    c1       = header['COEFF1']
    nwave    = header['NAXIS1']
    nfiber   = header['NAXIS2']
    wave     = (10**(c0 + c1*np.arange(nwave))).astype(np.float32)
    flux     = fx[0].data
    ivar     = fx[1].data
    and_mask = fx[2].data
    or_mask  = fx[3].data
    wavedisp = fx[4].data
    sky      = fx[6].data
    fx.close()

    if run1d is None:
        run1d = header['RUN2D']  #- default run1d == run2d

    #- Get best fit model from zbest file
    if zbestfile is None:
        zbestfile = platefile.replace('spPlate', '{}/spZbest'.format(run1d))

    model = fits.getdata(zbestfile, 2)

    coadds = list()
    for i in range(nfiber):
        sp = Table()
        sp['WAVE']     = wave               #- repeat !
        sp['FLUX']     = flux[i]
        sp['IVAR']     = ivar[i]
        sp['AND_MASK'] = and_mask[i]
        sp['OR_MASK']  = or_mask[i]
        sp['WAVEDISP'] = wavedisp[i]
        sp['SKY']      = sky[i]
        sp['MODEL']    = model[i]
        sp.meta = header

        #- TODO: Add units, comments to each column

        coadds.append(sp)

    return coadds

def load_frame(framefile, cframefile=None, flatfile=None):
    """
    Document ...
    """
    if cframefile is None:
        cframefile = framefile.replace('spFrame', 'spCFrame')
        if cframefile.endswith('.gz'):
            cframefile = cframefile[:-3]
    import os.path
    if os.path.exists(framefile)==False:
       print("v1,framefile:%s not exist"%framefile)
       #exit()
       spectra=list()
       return spectra
    #- Load framefile and get original dimensions
    eflux = fits.getdata(framefile, 0)
    nfiber, npix = eflux.shape
    if os.path.exists(cframefile)==False:
     print("v1,cframefile:%s not exist"%cframefile) 
     #exit()
     spectra=list()
     return spectra
    #- Load spCFrame file; trim arrays back to original size
    fx = fits.open(cframefile, memmap=False)
    header = fx[0].header
    flux = fx[0].data[:, 0:npix]
    ivar = fx[1].data[:, 0:npix]
    mask = fx[2].data[:, 0:npix]
    wave = (10**fx[3].data[:, 0:npix]).astype(np.float32)
    wavedisp  = fx[4].data[:, 0:npix]
    sky    = fx[6].data[:, 0:npix]
    x      = fx[7].data[:, 0:npix]
    superflat = fx[8].data[:, 0:npix]

    #- Load fiberflat spFlat[0]
    if flatfile is None:
        flatfile = header['FLATFILE'].replace('sdR', 'spFlat')
        flatfile = flatfile.replace('.fit', '.fits.gz')
        filedir, basename = os.path.split(os.path.abspath(cframefile))
        flatfile = os.path.join(filedir, flatfile)

    if os.path.exists(flatfile)==False:
      #exit()
      spectra=list()
      return spectra
    fiberflat = fits.getdata(flatfile, 0)

    #- Calculate calibration vector: flux = electrons * calib
    electrons = eflux * fiberflat * superflat
    ii = np.where(electrons != 0.0)
    calib = np.zeros(flux.shape)
    calib[ii] = flux[ii] / electrons[ii]

    fx.close()

    #- Assemble spectra tables
    spectra = list()
    for i in range(nfiber):
        sp = Table()
        sp['WAVE'] = wave[i]
        sp['FLUX'] = flux[i]
        sp['IVAR'] = ivar[i]
        sp['MASK'] = mask[i]
        sp['WAVEDISP'] = wavedisp[i]
        sp['SKY'] = sky[i]
        sp['X'] = x[i]
        sp['CALIB'] = calib[i].astype(np.float32)
        sp.meta = header

        #- TODO: Add units, comments to each column

        spectra.append(sp)

    return spectra


##Newly Added functions for converting into the stacked format.
def load_coadds_vstack(platefile,zbestfile=None, run1d=None):
    '''
    Document ...
    '''

    #- Load spPlate data
    fx = fits.open(platefile, memmap=False)
    header   = fx[0].header
    c0       = header['COEFF0']
    c1       = header['COEFF1']
    nwave    = header['NAXIS1']
    nfiber   = header['NAXIS2']
    wave     = (10**(c0 + c1*np.arange(nwave))).astype(np.float32)
    flux     = fx[0].data
    ivar     = fx[1].data
    and_mask = fx[2].data
    or_mask  = fx[3].data
    wavedisp = fx[4].data
    sky      = fx[6].data
    fx.close()
    run1d = header['RUN2D']  #- default run1d == run2d

    #- Get best fit model from zbest file
    zbestfile = platefile.replace('spPlate', '{}/spZbest'.format(run1d))
    model = fits.getdata(zbestfile, 2)
    coadds=(wave,flux,ivar,and_mask,or_mask,wavedisp,sky,model,header)
    return coadds
#copy each wavelength dataset into the hdf5 file
def write_coadds_vstack(platefile, plate,mjd,hdf5output,zbestfile=None, run1d=None):
    coadds=load_coadds_vstack(platefile, zbestfile=None, run1d=None)
    global dat
    outx=h5py.File(hdf5output,'a')
    for i in range(0,8):
        id = '{}/{}/coadds/{}'.format(plate, mjd, dat[i])
        print(id)
        try:
            dx=outx.create_dataset(id,data=coadds[i])
        except Exception as e:
            pass
        #temptb.write(hdf5output,id,append=True) #TODO: add table.meta to hdf5 attributes
        #dx.attrs.__setitem__(dat[i], coadds[8])
    #print(outx['7094/56660'].keys())
    outx.close()
#sections in load exposures
def load_frame_vstack(framefile,cframefile=None,flatfile=None):
    """
    Document ...
    """
    if cframefile is None:
        cframefile = framefile.replace('spFrame', 'spCFrame')
        if cframefile.endswith('.gz'):
            cframefile = cframefile[:-3]
    import os.path
    if os.path.exists(framefile)==False:
        print ("v2,framefile:%s not exist"%(framefile))
        exposure=tuple()
        return exposure
        #exit()
    #- Load framefile and get original dimensions
    eflux = fits.getdata(framefile, 0)
    nfiber, npix = eflux.shape
    if os.path.exists(cframefile)==False:
        print ("v2,cframefile:%s not exist"%(cframefile))
        exposure=tuple()
        return exposure
        #exit()
    #- Load spCFrame file; trim arrays back to original size
    fx = fits.open(cframefile, memmap=False)
    header = fx[0].header   # this means the exposureid is same for flux, ivar, etc
    expid=header['EXPOSURE']
    flux = fx[0].data[:, 0:npix]
    ivar = fx[1].data[:, 0:npix]
    mask = fx[2].data[:, 0:npix]
    wave = (10**fx[3].data[:, 0:npix]).astype(np.float32)
    wavedisp  = fx[4].data[:, 0:npix]
    sky    = fx[6].data[:, 0:npix]
    x      = fx[7].data[:, 0:npix]
    superflat = fx[8].data[:, 0:npix]

    #- Load fiberflat spFlat[0]
    if flatfile is None:
        flatfile = header['FLATFILE'].replace('sdR', 'spFlat')
        flatfile = flatfile.replace('.fit', '.fits.gz')
        filedir, basename = os.path.split(os.path.abspath(cframefile))
        flatfile = os.path.join(filedir, flatfile)

    if os.path.exists(flatfile)==False:
        print("%s not exist"%(flatfile))
        exit()
    fiberflat = fits.getdata(flatfile, 0)

    #- Calculate calibration vector: flux = electrons * calib
    electrons = eflux * fiberflat * superflat
    ii = np.where(electrons != 0.0)
    calib = np.zeros(flux.shape)
    calib[ii] = flux[ii] / electrons[ii]

    fx.close()
    exposure=(wave,flux,ivar,mask,wavedisp,sky,x,calib.astype(np.float32), header,expid)
    return exposure

def combine_write_frame(frame1,frame2,expid,plate,mjd,hdf5output,br):
    #print ('dump:',expid)
    global expdat
    try: 
        outx=h5py.File(hdf5output,'a')
        #print(outx)
    except Exception as e:
        print ("file open error")
        traceback.print_exc()
        pass
    #print("len expdat:%d"%(len(expdat)))
    for i in range(0,len(expdat)):
        id = '{}/{}/exposures/{}/{}/{}'.format(plate, mjd, expid,br,expdat[i])
        #print (id)
        try:
            dset=np.append(frame1[i],frame2[i],axis=0)
            #print ("id:%s"%id)
            #print ("shape:",dset.shape)
        except Exception as e:
            print ("append error")
            trackback.print_exc()
            pass
        try:
            dx=outx.create_dataset(id,data=dset)
            #print(dx)
        except Exception as e:
            print ('error in frame dump')
            trackback.print_exc()
            pass
        #outx.flush()
    try:
        print("done writing exposure %d, br:%s"%(expid,br))
        outx.close()
    except Exception as e:
        print ("file close error")
        traceback.print_exc()
def write_frame(frame1,expid,plate,mjd,hdf5output,br):
    #print ("single dump:",expid)
    global expdat
    try:
        outx=h5py.File(hdf5output,'a')
        print (outx)
    except Exception as e:
        print ("file open error")
        traceback.print_exc()
        pass
    print("len expdat:%d"%(len(expdat)))
    for i in range(0,len(expdat)):
        id = '{}/{}/exposures/{}/{}/{}'.format(plate, mjd, expid,br,expdat[i])
        #print(id)
        dset=frame1[i]
        try:
            #print ("creating dataset in frames")
            dx=outx.create_dataset(id,data=dset)
            #print(dx)
        except Exception as e:
            print ('error in frame dump')
            trackback.print_exc()
            pass
    try:
       print("done writing exposure %d, br:%s"%(expid,br))
       outx.close()
    except Exception as e:
       print ("file close error")
       trackback.print_exc()
def write_frame_vstack(filedir,framefiles,plate,mjd,hdf5output, cframefile=None, flatfile=None):
    frameb1=list()
    framer1=list()
    frameb2=list()
    framer2=list()
    ii=0
    #print("length of framefiles:%d"%(len(framefiles)))
    for filename in framefiles:
        #print ("iteration:%d"%(ii))
        ii=ii+1
        offset = 0  #fiber 0-499
        if ('spFrame-b1' in filename):
            try:
             frame=load_frame_vstack(filedir+'/'+filename)
             if len(frame)!=0:
              frameb1.append(frame)
             #print (filename, frame[9])
            except Exception as e:
                print ("File not found")
                traceback.print_exc()
                pass
        if ('spFrame-r1' in filename):
            try:
             frame=load_frame_vstack(filedir+'/'+filename)
             if len(frame)!=0:
              framer1.append(frame)
             #print (filename, frame[9])
            except Exception as e:
                print ("File not found")
                traceback.print_exc()
                pass
    print ("finish loading b1, r1")
    for filename in framefiles:
        offset = 500 #fiber 500-999
        if ('spFrame-b2' in filename):
            try:
             frame=load_frame_vstack(filedir+'/'+filename)
             if len(frame)!=0:
              frameb2.append(frame)
             #print (filename, frame[9])
            except Exception as e:
                print ("File not found")
                pass
        if ('spFrame-r2' in filename):
            try:
             frame=load_frame_vstack(filedir+'/'+filename)
             if len(frame)!=0:
              framer2.append(frame)
             #print (filename, frame[9])
            except Exception as e:
                print ("File not found")
                pass
    print("finish loading b2,r2")
    #combine b1 and b2
    print ("frameb1:%d"%(len(frameb1)))
    for i in range(0,len(frameb1)):
        expidb1=frameb1[i][9]
        #print("expidb1:%d"%expidb1)
        hit=0
        for j in range(0,len(frameb2)):
            expidb2=frameb2[j][9]
            #print("expidb2:%d"%expidb2)
            if expidb1==expidb2:
                #combine frameb1 and frameb2
                combine_write_frame(frameb1[i],frameb2[j],expidb1,plate,mjd,hdf5output,'b')
                try:
                 frameb2.remove(frameb2[j])
                except Exception as e:
                 traceback.print_exc()
                hit=1
                break
        if hit==0:
            #print ("hit is 0")
            write_frame(frameb1[i],expidb1,plate,mjd,hdf5output,'b')
    for i in range(0,len(frameb2)):
        #print("leftover frameb2:%d"%(frameb2))
        expidb2=frameb2[i][9]
        write_frame(frameb2[i],expidb2,plate,mjd,hdf5output,'b')

    #combine r1 and r2
    print ("framer1:%d"%(len(framer1)))
    for i in range(0,len(framer1)):
        expidr1=framer1[i][9]
        hit=0
        for j in range(0,len(framer2)):
            expidr2=framer2[j][9]
            if expidr1==expidr2:
                #combine frameb1 and frameb2
                combine_write_frame(framer1[i],framer2[j],expidr1,plate,mjd,hdf5output,'r')
                framer2.remove(framer2[j])
                hit=1
                break
        if hit==0:
            write_frame(framer1[i],expidr1,plate,mjd,hdf5output,'r')
    for i in range(0,len(framer2)):
        #print("leftover:%d"%(len(framer2)))
        expidr2=framer2[i][9]
        write_frame(framer2[i],expidr2,plate,mjd,hdf5output,'r')
