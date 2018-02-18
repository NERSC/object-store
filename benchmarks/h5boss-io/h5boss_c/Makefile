CC=cc
CFLAGS=-g -c -Wall
LDFLAGS=
SOURCES=subset.c parse_node.c compound_copy.c
OBJECTS=$(SOURCES:.c=.o)
EXECUTABLE=subset.exe

all: $(SOURCES) $(EXECUTABLE)
    
$(EXECUTABLE): $(OBJECTS) 
	$(CC)	$(LDFLAGS) $(OBJECTS) -g -o $@

.c.o:
	$(CC) $(CFLAGS) $< -o $@
clean:
	rm *.o subset.exe
