CFLAGS = -c -fno-strict-aliasing -Wmissing-include-dirs -O2 -DNDEBUG -fwrapv -O3 -Wall -Wstrict-prototypes -I/usr/common/software/python/2.7-anaconda-4.4/include/python2.7 -isystem/usr/common/software/python/2.7-anaconda-4.4/lib/python2.7/site-packages/numpy/core/include  -Wno-unused-variable  -Wno-strict-prototypes -Wno-cpp -I/usr/common/software/hdf5-parallel/hdf5/src
LDFLAGS = -L/usr/common/software/python/2.7-anaconda-4.4/lib -lpython2.7 -lpthread -ldl -lutil -lm -Xlinker -export-dynamic #-Wl,--verbose

ALL:	VPIC_IO

VPIC_IO: python_vol.o vpicio_uni_h5.o
	h5pcc -g vpicio_uni_h5.o python_vol.o $(LDFLAGS) -o VPIC_IO
vpicio_uni_h5.o: vpicio_uni_h5.c
	h5pcc -g $(CFLAGS) vpicio_uni_h5.c -o vpicio_uni_h5.o
python_vol.o: ${dir}/python_vol.c
	h5pcc -g $(CFLAGS) python_vol.c -o python_vol.o
clean:
	rm VPIC_IO vpicio_uni_h5.o python_vol.o
