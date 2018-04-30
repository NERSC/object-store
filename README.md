# Object-Store in HPC <img src="https://user-images.githubusercontent.com/1396867/39416409-8a74adee-4c01-11e8-9453-07031099f6e6.png" width="60">

Evaluating object stores with HPC science applications


**Object Stores** and its HPC supported version:

|  | Swift     | DAOS        | Ceph  |
| :------------- | -------------: |-------------:| -----:|
|Cloud  | [Openstack Swift](https://github.com/openstack/swift)      | [DAOS](https://github.com/daos-stack/daos) |[RADOS](http://docs.ceph.com/docs/master/rados/) |
|HPC  | [HDF5_Swift-VOL](https://github.com/valiantljk/sci-swift)     | [HDF5_DAOS-VOL](https://bitbucket.hdfgroup.org/users/nfortne2/repos/hdf5_naf/browse?at=refs%2Fheads%2Fhdf5_daosm)|  [HDF5_RADOS-VOL](https://bitbucket.hdfgroup.org/users/nfortne2/repos/hdf5_naf/browse?at=refs%2Fheads%2Fhdf5_rados) |




**Science Applications** that represent typical HPC workload:

|| BOSS       | VPIC           | BDCATS   |
|------------- | ------------- |:-------------:| -----:|
|Field| Astronomy      | Physics |  Plasma |
|Code| [H5BOSS](https://github.com/valiantljk/h5boss)     | [VPIC-IO](https://sdm.lbl.gov/exahdf5/software.html)     |   [BDCATS-IO](https://sdm.lbl.gov/exahdf5/software.html) |


**Testbed**:

|| Swift@NERSC       | Boro@Intel           | Ceph@NERSC   |
|------------- | ------------- |:-------------:| -----:|
|Storage Nodes| 4     | 8|  4 |
|Gateway Nodes| 2    |  NA    | 2   |
|Compute Nodes|12076|66|12076|
|Ram per Compute Node(GB)| 96-128|128|96-128|
|Cores per Compute Node|32-68|32|32-68|
|Capacity per Storage Node (TB)|280|2|280|
|Cpu|Haswell/KNL|Haswell|Haswell/KNL|


**How To Use Now**

1. code modification, e.g., line [39](https://github.com/NERSC/object-store/blob/master/bench_obj_test/ceph/vpic_io/vpicio_uni_h5.c#L39), [145-160](https://github.com/NERSC/object-store/blob/master/bench_obj_test/ceph/vpic_io/vpicio_uni_h5.c#L145)
2. recompile
```
module load hdf5-parallel/rados
h5pcc -o vpic vpic.c
./vpic 
```
**How To Use in the Future**:

```
module load rados/daos/swift
./vpic 
```

**Metrics**

Use at Performance Tier vs. Lustre

|Metrics| Function/Scale|
|-------|-------|
|Bandwidth|single node, multi-nodes(scalability)|
|IOPS||
|Metadata|File/object open/close/create|
|Autentication||

Use at Warm/Cold Tier vs. HPSS?

|Metrics||
|-------|------|
|File Scan|ls|
|File Movement|cp, put/get|
|File Sharing||
|User-defined Metadata|put/get|

Transition (Posix to Object API)

|Metrics|
|-------|
|?|

Admin/ Operation 

|Metrics|
|------|
|Live Expandability|
|Relibility|
|Rebuilding|
