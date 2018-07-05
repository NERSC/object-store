all: BDCATS BDCATS_LUSTRE

BDCATS:bdcatsio_rados.c 
	h5pcc -o BDCATS bdcatsio_rados.c 
	cp BDCATS script
BDCATS_LUSTRE: bdcatsio_lustre.c
	h5pcc -o BDCATS_LUSTRE bdcatsio_lustre.c
clean:
	rm BDCATS *.o BDCATS_LUSTRE 
