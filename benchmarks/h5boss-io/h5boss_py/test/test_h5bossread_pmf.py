from __future__ import division, print_function
import numpy as np
import h5py
import sys
sys.path.append('/global/homes/j/jialin/.local/edison/2.7-anaconda/lib/python2.7/site-packages')
import time
import fitsio
import traceback
import optparse
import argparse
import pandas as pd
fitserr=0
coaderr=0
checker=0
h5file=0
fopentot=0
dreadtot=0
boss_dir="/global/projecta/projectdirs/sdss/data/sdss/dr12/boss/spectro/redux/v5_7_0/"
h5boss_dir="/global/cscratch1/sd/jialin/h5boss/"
h5boss_v2_dir="/global/cscratch1/sd/jialin/h5boss_v2/"
i_wave={"flux": 1,"ivar": 0,"andmask": 2, "ormask": 3, "wavedisp": 4, "sky": 6} # digit is the HDU number
h5b_wave={'WAVE', 'FLUX', 'IVAR', 'AND_MASK', 'OR_MASK', 'WAVEDISP', 'SKY', 'MODEL'} # dx.dtype.names
h5b_wave2={'and_mask', 'flux', 'ivar', 'model', 'or_mask', 'sky', 'wave', 'wavedisp'} # fx['plate/mjd/coadds'].keys. for reference

from memory_profiler import profile
 
def parsePMF(pmflist):
    try:
        df = pd.read_csv(pmflist,delimiter=' ',names=["plates","mjds","fibers"],index_col=None,dtype=str,header=0)
        df = df.sort_values(by="plates",ascending=[1])
        plates = list(map(str,df['plates'].values))
        mjds = list(map(str,df['mjds'].values))
        fibers = list(map(str,df['fibers'].values))
    except Exception as e:
        print("pmf csv read error or not exist:%s"%e,pmflist)
        print("e.g., 1st row of csv should be 'plates mjds fibers'")
    return (plates,mjds,fibers)
#@profile
def h5bread_s_pmf(fx,plate,mjd,fiber,wave):
    '''
       read the pmf from pre-selected hdf5 file, version 1
       
    '''
    global fitserr, coaderr,checker,dreadtot
    dwave=0
    pmfcad=plate+"/"+mjd+"/"+fiber+"/coadd"
    dread=time.time()
    try:
     #if pmfcad in fx.keys():
     if wave=="all":
      dwave=fx[pmfcad][:]
      checker=checker+dwave[0][0]
     else:
      dwave=fx[pmfcad][wave]
      #print (dwave.shape,dwave)
      checker=checker+dwave[300]
      #else:
      #  print ("pmfcad %s not found in pre file %s"%(pmfcad,fx))
    except Exception as e:
      #traceback.print_exc()
      coaderr=coaderr+1
      pass
    dreadtot=dreadtot+time.time()-dread
    return dwave

def h5bread_s2_pmf(fx,plate,mjd,fiber,wave):
    '''
       read the pmf from pre-selected hdf5 file, version 2
       
    '''
    global fitserr, coaderr,checker,dreadtot
    dwave=0
    pmfcad=plate+"/"+mjd+"/coadds/"+wave
    dread=time.time()
    try:
     if wave=="all":
      for iiwave in h5b_wave2:
       pmfcad=plate+"/"+mjd+"/coadds/"+iiwave
       #dwave=fx[pmfcad][int(fiber)-1]
       dwave=fx[pmfcad][0] #TODO: version 2, catalog table doesn't contain the index of fiber, needs to modify the catalog's plugmap table and inser the fiber id in one of its column
       #without knowing the index, we just extract all fibers's data. The 'plate/mjd'/fiber should be unique first, though, without the help of indexing we just get the first fiber for simplicity
       if iiwave=="flux":
        checker=checker+dwave[300] 
     else:
      #dwave=fx[pmfcad][int(fiber)-1]
      
      dwave=fx[pmfcad][0]
      #print (dwave.shape,dwave)
      checker=checker+dwave[300]
    except Exception as e:
      coaderr=coaderr+1
      pass
    dreadtot=dreadtot+time.time()-dread
    return dwave


def h5bread_pmf(plate,mjd,fiber,wave):
    '''
        read the pmf from source hdf5 files, to have an apple2apple comparison with fitsread_pmf
        para: plate, mjd, fiber
        para: wave is the name of wavelength variable
        return: fiber-th row in dataset wave
    '''
    dwave=0
    global fitserr, coaderr, checker
    fopen=time.time()
    try:
        h5bfile=h5boss_dir+plate+"-"+mjd+".hdf5"
        dh5=h5py.File(h5bfile,'r')
    except Exception as e:
        #traceback.print_exc()
        fitserr=fitserr+1
        #print("HDF5 open error %s"%h5bfile)
        pass
    global fopentot
    fopentot=fopentot+time.time()-fopen
        #pass 
    dread=time.time()
    try:
        #print ("%s-%s,%s,fiber %s"%(plate,mjd,wave,fiber))
        pmfcad=plate+"/"+mjd+"/"+fiber+"/coadd"
        dwave=dh5[pmfcad][wave] # only access one wavelength, i.e., 1 column 
        #print (dwave.shape,dwave)
        checker=checker+dwave[300]
        global h5file
        h5file=h5file+1
    except Exception as e:
        #traceback.print_exc()
        #print("extraction error %s-%s %s"%(plate,mjd,fiber))
        coaderr=coaderr+1
        pass
    global dreadtot
    dreadtot=dreadtot+time.time()-dread
    return dwave
def h5bread_2_pmf(plate,mjd,fiber,wave):
    '''
        read the pmf from source hdf5 files, version 2, to have an apple2apple comparison with fitsread_pmf
        para: plate, mjd, fiber
        para: wave is the name of wavelength variable
        return: fiber-th row in dataset wave
    '''
    dwave=0
    global fitserr, coaderr, checker
    fopen=time.time()
    mk=0
    try:
        h5bfile=h5boss_v2_dir+plate+"-"+mjd+".hdf5"
        dh5=h5py.File(h5bfile,'r')
    except Exception as e:
        #traceback.print_exc()
        fitserr=fitserr+1
        mk=1
        print("HDF5 open error %s"%h5bfile)
        pass
    global fopentot
    fopentot=fopentot+time.time()-fopen
        #pass 
    dread=time.time()
    dwave=0
    if mk==0:
      try:
        #print ("%s-%s,%s,fiber %s"%(plate,mjd,wave,fiber))
        #pmfcad=plate+"/"+mjd+"/"+fiber+"/coadd"
        pmfcad=plate+"/"+mjd+"/coadds/"+wave
        dwave=dh5[pmfcad][int(fiber)-1] # only access one wavelength, i.e., 1 column 
        #print (dwave.shape,dwave)
        
        checker=checker+dwave[300]
        global h5file
        h5file=h5file+1
      except Exception as e:
        traceback.print_exc()
        print("extraction error:pmfcad:%s %s-%s %s,file:%s"%(pmfcad,plate,mjd,fiber,h5bfile))
        coaderr=coaderr+1
        pass
    global dreadtot
    dreadtot=dreadtot+time.time()-dread
    return dwave

#@profile
def test_h5bread_pmf(pmflist,var,v):
    '''
      interface for h5bread_pmf, source files
    '''
    try:
        (plate,mjd,fiber)=parsePMF(pmflist)
    except Exception as e:
        traceback.print_exc()
        print("parse pmf: %s error"%pmflist)
    print ("reading %d pmf"%len(plate))
    st=time.time()
    try:
        for i in range(0,len(plate)):
            if v == "1":
              h5bread_pmf(plate[i],mjd[i],fiber[i],var)
            elif v=="2":
              h5bread_2_pmf(plate[i],mjd[i],fiber[i],var)
    except Exception as e:
        #traceback.print_exc()
        #print("read fiber %s error %s"%(fiber[i],var))
        pass
    global h5file,fopentot,dreadtot
    print ("h5boss_src:version:%s:h5file %d, Total: %.2f seconds, File Open: %.2f, Data Read: %.2f"%(v,h5file,time.time()-st,fopentot,dreadtot))
    global fitserr, coaderr, checker
    print ("hdfserr: %d, coaderr: %d, checker: %f"%(fitserr,coaderr,checker))

def test_h5bread_s_pmf(pmflist,var,pre,v):
    '''
       interface for h5bread_s_pmf, pre-selected file
    '''
    try:
        (plate,mjd,fiber)=parsePMF(pmflist)
    except Exception as e:
        traceback.print_exc()
        print("parse pmf: %s error"%pmflist)
    print ("reading %d pmf"%len(plate))
    st=time.time()
    try:
 	fx=h5py.File(pre,'r')
    except Exception as e:
        print("pre file open error %s"%pre)
        sys.exit()
    fileopentot=time.time()-st
    try:
        if v=="1":
         for i in range(0,len(plate)):
             h5bread_s_pmf(fx,plate[i],mjd[i],fiber[i],var)
        elif v=="2":
         for i in range(0,len(plate)):
             h5bread_s2_pmf(fx,plate[i],mjd[i],fiber[i],var)         
    except Exception as e:
        #traceback.print_exc()
        #print("read fiber %s error %s"%(fiber[i],var))
        pass
    fileclose=time.time()
    try:
        fx.close()
    except Exception as e:
        print("pre file close error")
    fileclosetot=time.time()-fileclose
    global dreadtot
    print ("h5boss_pre:version %s:Total Cost: %.2f seconds, File Open: %.2f, Data Read: %.2f, File Close: %.2f"%(v,time.time()-st,fileopentot,dreadtot,fileclosetot))
    global fitserr, coaderr, checker
    print ("hdfserr: %d, coaderr: %d, checker: %f"%(fitserr,coaderr, checker))
if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='h5bread')
    parser.add_argument("pmf",    help="Plate/mjd/fiber list in csv")
    parser.add_argument("var",    help="Wavelength name: version1:WAVE,FLUX,IVAR,AND_MASK,OR_MASK,WAVEDISP,SKY,MODEL, version2:and_mask,flux,ivar,model,or_mask,sky,wave,wavedisp")
    parser.add_argument("pre",	  help="pre-selected h5boss file: filename or 'none' means will read from source files")
    parser.add_argument("v",    help="h5boss version")
    opts=parser.parse_args()
    pmflist = opts.pmf
    var = opts.var.split(',')
    ver = opts.v
    pre = opts.pre
    for i in range(0,len(var)):
     if pre == 'none':
       print ("read from source files,version:%s"%ver)
       test_h5bread_pmf(pmflist,var[i],ver)
     else: 
       print ("read from pre-selected file %s,version:%s"%(pre,ver))
       test_h5bread_s_pmf(pmflist,var[i],pre,ver) 
