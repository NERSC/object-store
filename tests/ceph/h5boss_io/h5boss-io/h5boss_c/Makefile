CC=h5pcc
CFLAGS= -g -O0
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
	rm  h5boss_rados h5boss_lustre
