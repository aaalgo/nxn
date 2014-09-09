CC=g++
CXX=g++
CXXFLAGS += -g  -Wall -O3 -I..
#-O3 -fopenmp
LDFLAGS += -O3 -L..
LDLIBS += -lboost_program_options -lnxn -lrt -lm -lz -llzo2 -lpthread

.PHONY:	all clean

HEADER = *.h
COMMON = 

PROGS = knng scan

all:	$(PROGS)

clean:
	rm -f *.o $(PROGS)

