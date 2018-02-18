import h5py
import numpy as np
import time

def printname(name):
    print name

## how many spectra to read? 
nspectra = 1000
totnspectra = 1000

### first, read the pmf file
pmf = np.zeros( (nspectra,3))
i = -1
for line in open("pmf-list/pmf1k"):
    if "plates" in line:
        continue
    cols = line.split()

    i+=1
    if i>nspectra:
        continue

    pmf[i][0] = int(cols[0])
    pmf[i][1] = int(cols[1])
    pmf[i][2] = int(cols[2])

print pmf.shape
print pmf[0]

## what's in teh file?
#file.visit(printname)

## this goes plate/mjd/fiber. I want to pull out spectrum - so plate/mjd/fiber/coadd/...
spectra = []
i=0

### timeing: open file, extract spectra, put into the array       
t1 = time.clock()
t4=time.time()
filename = "/scratch1/scratchdirs/jialin/bosslover/scaling-test/1k.h5"
file    = h5py.File(filename, 'r')

for i in range(nspectra):
    
    datasetname = str(pmf[i][0])[:-2]+"/"+str(pmf[i][1])[:-2]+"/"+str(pmf[i][2])[:-2]+"/coadd"
 
    try:
        dataset = file[datasetname]
        #print (time.clock())
        #print (time.time())
    except:
        print "******* dataset not found! **************", datasetname
        continue

    #print dataset.dtype ### spits out the params in the dataset
    spectra.append(dataset["FLUX"])
t2 = time.clock()
t3 = time.time()
spectra = np.array(spectra)
print spectra.shape
print spectra[0]
print spectra.nbytes
print "wall time: %.2f seconds"%(t3-t4)
print "time taken to load all the spectra into an array: %.2f sec"%(t2-t1)
#print "wall time: %.2f seconds"%(t3-t4)

