CC=h5pcc
CFLAGS=  -Wall
LDFLAGS=
SOURCES=h5boss_rados.c h5boss_lustre.c
OBJECTS=$(SOURCES:.c=.o)
EXECUTABLE=h5boss_rados h5boss_lustre

all: h5boss_rados h5boss_lustre
h5boss_rados:h5boss_rados.c
	$(CC) $(CFLAGS) -o $@ $<
h5boss_lustre:h5boss_lustre.c
	 $(CC) $(CFLAGS) -o  $@ $<
	 cp $(EXECUTABLE) script
clean:
	rm *.o h5boss_rados h5boss_lustre
