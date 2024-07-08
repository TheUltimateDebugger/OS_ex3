CC=g++
CXX=g++

CODESRC=MapReduceFramework.cpp
EXEOBJ=libMapReduceFramework.a

INCS=-I.
CFLAGS = -Wall -std=c++11 -O3 $(INCS)
CXXFLAGS = -Wall -std=c++11 -O3 $(INCS)

TARGETS = $(EXEOBJ)

TAR=tar
TARFLAGS=-cvf
TARNAME=ex3.tar
TARSRCS= Makefile README MapReduceFramework.cpp

all: $(TARGETS)

$(EXEOBJ): uthreads.o
	ar rcs $@ $^

uthreads.o: uthreads.cpp
	$(CXX) $(CXXFLAGS) -c $<

clean:
	$(RM) $(TARGETS) uthreads.o

depend:
	makedepend -- $(CFLAGS) -- $(CODESRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
