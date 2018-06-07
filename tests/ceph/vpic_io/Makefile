all: VPIC rados_write

VPIC:vpicio_uni_h5.c
	h5pcc -o VPIC vpicio_uni_h5.c
rados_write:librados_test.c
	h5pcc -o rados_write librados_test.c
clean:
	rm VPIC *.o rados_write
