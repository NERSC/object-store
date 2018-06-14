all: VPIC librados_test

VPIC:vpicio.c
	h5pcc  -o VPIC vpicio.c
	cp VPIC scripts/
librados_test:librados_test.c
	h5pcc -o librados_test librados_test.c
	cp librados_test scripts/
clean:	
	rm VPIC *.o librados_test
