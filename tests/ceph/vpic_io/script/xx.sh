#!/bin/bash
srun -n 64 ./VPIC boro64_1.h5 swiftpool $CEPH_CONF 26214400 4194304
wait
srun -n 64 ./VPIC boro64_2.h5 swiftpool $CEPH_CONF 26214400 8388608 
wait
srun -n 64 ./VPIC boro64_3.h5 swiftpool $CEPH_CONF 26214400 16777216
