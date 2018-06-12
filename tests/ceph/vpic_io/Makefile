all: VPIC librados_test

VPIC:vpicio.c
	h5pcc  -o VPIC vpicio.c
librados_test:librados_test.c
	h5pcc -o librados_test librados_test.c

clean:	
	rm VPIC *.o librados_test
