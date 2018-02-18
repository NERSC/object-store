H5PART_ROOT=$(HOME)/apps.cori-knl/H5Part-1.6.6-intel-18API

CFLAGS  = -DPARALLEL_IO -I$(H5PART_ROOT)/include
LDFLAGS = -L$(H5PART_ROOT)/lib 
LDLIBS  = -lH5Part

.PHONY: all clean

BINARIES = vpicio_uni vpicio_uni_dyn

all: $(BINARIES)

vpicio_uni_dyn: LDFLAGS += -dynamic
vpicio_uni_dyn: vpicio_uni.o
	$(CC) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@

clean:
	rm -f *.o $(BINARIES)
