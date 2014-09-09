.PHONY:	all clean

CC = gcc
CFLAGS += -O3 -g -Wall -std=c99 -D_GNU_SOURCE 
LDLIBS += -lpthread -lrt -lncurses -llzo2 # -lz

SHARED = impl.o queue.o mon.o util.o
LIB = libnxn.a
PROG = nxn nxn-slave nxn-tools nxn-mon nxn-busy # nxn-test

all:	$(LIB) $(PROG)
	make -C knng clean
	make -C knng

libnxn.a:	$(SHARED)
	ar cr $@ $^

nxn:	nxn.o	$(LIB)

nxn-mon:	nxn-mon.o $(LIB) 

nxn-busy:	nxn-busy.o $(LIB) 

nxn-slave:	nxn-slave.o $(LIB)

nxn-test:	nxn-test.o $(LIB)

nxn-tools:	nxn-tools.o $(LIB)

clean:
	rm -rf *.o $(LIB) $(PROG)

