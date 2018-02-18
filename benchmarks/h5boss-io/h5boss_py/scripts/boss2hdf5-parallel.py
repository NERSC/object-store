import os
from h5boss import boss2hdf5
import traceback
import argparse
from mpi4py import MPI

datapath = "/global/projecta/projectdirs/sdss/data/sdss/dr12/boss/spectro/redux/v5_7_0/"
#outputpath= "/global/cscratch1/sd/jialin/h5boss-bp/"
outputpath= "/global/cscratch1/sd/jialin/h5boss_v2/"

def listfiles():
     ldir=os.listdir(datapath)
     lldir=[fn for fn in ldir if fn.isdigit()]
     return lldir

def findseed(x):
     fitsfiles = [os.path.join(root, name)
       for root, dirs, files in os.walk(x)
       for name in files
        if name.startswith("spPlate") and name.endswith(".fits")]
     return fitsfiles

def parallel_convert():
    parser = argparse.ArgumentParser(prog='convert')
    parser.add_argument("--input", type=str,  help="input fits directory")
    parser.add_argument("--output", type=str,  help="output hdf5")

    opts = parser.parse_args()
    global datapath
    global outputpath
    if opts.input:
    	datapath = opts.input

    if opts.output:
    	outputpath = opts.output

    rank = MPI.COMM_WORLD.Get_rank()
    nproc = MPI.COMM_WORLD.Get_size()
    plateslist=listfiles()
    if (nproc>len(plateslist)):
        nproc=len(plateslist)
    plateslist_for_each_process = [fname for fname in plateslist[:nproc]]
    if(rank==0):
        print "nproc:%d\n"%nproc
    print "\nrank :%d\n"%rank
    if(rank<nproc):
        platepath_for_current_rank = datapath+plateslist_for_each_process[rank]
        fitspath_name_for_current_rank = findseed(platepath_for_current_rank)
        if(len(fitspath_name_for_current_rank)>0):
            hdf5file=fitspath_name_for_current_rank[0].split('/')[-1].replace('spPlate-','',1).replace('fits','hdf5',1)
            try:
             boss2hdf5.serial_convert(fitspath_name_for_current_rank,outputpath+hdf5file)
            except Exception, e:
             print "Error:%s"%e, fitspath_name_for_current_rank
             traceback.print_exc()
if __name__ == '__main__':
    parallel_convert()
