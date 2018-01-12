# Object-Store in HPC
Evaluating object stores for HPC science applications


**Object Stores** and its HPC friendly version:

|  | Swift     | DAOS        | Ceph  |
| :------------- | -------------: |-------------:| -----:|
|Cloud  | [Openstack Swift](https://github.com/openstack/swift)      | [DAOS](https://github.com/daos-stack/daos) |[RADOS](http://docs.ceph.com/docs/master/rados/) |
|HPC  | [Sci-Swift](https://github.com/valiantljk/sci-swift)     | [DAOS-M](https://bitbucket.hdfgroup.org/users/nfortne2/repos/hdf5_naf/browse?at=refs%2Fheads%2Fhdf5_daosm)|  [RADOS-VOL](https://github.com/ir193/hdf5-vol-ceph) |




**Science Applications** that represent typical HPC workload:

|| BOSS       | VPIC           | HACC   |
|------------- | ------------- |:-------------:| -----:|
|Field| Astronomy      | Physics |  Cosmology |
|Code| [H5BOSS](https://github.com/valiantljk/h5boss)     | [VPIC-IO](https://github.com/glennklockwood/vpic-io)     |   [HACC-IO](https://asc.llnl.gov/CORAL-benchmarks/#hacc) |
