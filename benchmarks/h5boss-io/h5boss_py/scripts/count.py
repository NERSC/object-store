import os,sys

def listfiles(datapath):
     ldir=os.listdir(datapath)
     lldir=[fn for fn in ldir if fn.isdigit()]
     return lldir

def findseed(x):
     fitsfiles = [os.path.getsize(os.path.join(root, name))
       for root, dirs, files in os.walk(x)
       for name in files
        #if name.startswith(("spPlate","spCFrame","spFlat","spZbest","spZline","photoPlate","photoMatchPlate","photoPosPlate")) and name.endswith((".fits",".fits.gz"))]
         if name.endswith((".fits",".fits.gz"))]
     return sum(fitsfiles)/1024.0/1024.0/1024.0

def count_fits():
    datapath="/global/projecta/projectdirs/sdss/data/sdss/dr12/boss/spectro/redux/v5_7_0/"
    lfd=listfiles(datapath)
    print ("number of plate folder:%d"%len(lfd))
    tfits=0
    for i in range(0,len(lfd)):
     xf=lfd[i]
     xfn=findseed(datapath+str(xf))
     print ("%.2f"%xfn)
     tfits+=xfn
    print ("Total:%.2f GB"%tfits)
if __name__=="__main__":
   count_fits()

