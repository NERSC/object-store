from __future__ import division, print_function
import numpy as np
import sys
sys.path.append('/global/homes/j/jialin/.local/edison/2.7-anaconda/lib/python2.7/site-packages')
import time
import fitsio
import traceback
import optparse
import argparse
import pandas as pd
from memory_profiler import profile
fitserr=0
coaderr=0
totfiber=0
dreadtot=0
fileopentot=0
#boss_dir="/global/projecta/projectdirs/sdss/data/sdss/dr12/boss/spectro/redux/v5_7_0/"
boss_dir="/global/cscratch1/sd/jialin/bossfits/"
i_wave={"flux": 0,"ivar": 1,"andmask": 2, "ormask": 3, "wavedisp": 4, "sky": 6}
checker=0
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
def fitsread_pmf(plate,mjd,fiber,wave):
    '''
        para: plate, mjd, fiber
        para: wave is the name of wavelength variable
        return: fiber-th row in dataset wave
    '''
    dwave=0
    global fitserr,coaderr,checker,fileopentot,dreadtot
    fileopen=time.time()
    try:
        fitsfile=boss_dir+plate+"/spPlate-"+plate+"-"+mjd+".fits"
        dfits=fitsio.FITS(fitsfile)
    except Exception as e:
        #traceback.print_exc()
        fitserr=fitserr+1
        #print("fits open error %s"%fitsfile)
        pass 
    fileopentot=fileopentot+time.time()-fileopen
    dread=time.time()
    try:
        di=int(i_wave[wave])
        #print ("%s-%s,%s,fiber %s"%(plate,mjd,wave,fiber))
        dwave=dfits[di][int(fiber)-1:int(fiber),:]
        #print (dwave.shape,dwave)
        checker=checker+dwave[0,300]
        global totfiber
        totfiber=totfiber+1
    except Exception as e:
        #traceback.print_exc()
        coaderr=coaderr+1
        #print("extraction error %s-%s %s"%(plate,mjd,fiber))
        pass
    dreadtot=dreadtot+time.time()-dread
    return dwave


def test_fitsread_pmf(pmflist,var):
    try:
        (plate,mjd,fiber)=parsePMF(pmflist)
    except Exception as e:
        #traceback.print_exc()
        print("parse pmf: %s error"%pmflist)
    print ("reading %d pmf"%len(plate))
    st=time.time()
    try:
        for i in range(0,len(plate)):
            fitsread_pmf(plate[i],mjd[i],fiber[i],var)
    except Exception as e:
        #traceback.print_exc()
        #print("read fiber %s error %s"%(fiber[i],var))
        pass
    global totfiber,fileopentot,dreadtot
    print ("Fitsio: %d fits files Total cost: %.2f seconds, File Open: %.2f, Data Read: %.2f"%(totfiber,time.time()-st,fileopentot,dreadtot))
    global fitserr,coaderr,checker
    print ("fitserr: %d, coaderr: %d, checker: %f"%(fitserr,coaderr,checker))
if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='fitsread')
    parser.add_argument("pmf",    help="Plate/mjd/fiber list in csv")
    parser.add_argument("var",    help="Wavelength name, sep with comma")
     
    opts=parser.parse_args()
    pmflist = opts.pmf
    var = opts.var.split(',')
    for i in range(0,len(var)):
     test_fitsread_pmf(pmflist,var[i])
