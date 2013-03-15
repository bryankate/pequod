.SUFFIXES: .o .c .cpp

CC=g++
cc=gcc

BIN=orhn

ICINCHOME=/opt/instantclient/sdk/
ICLIBHOME=/opt/instantclient/
ICLIBPATH=-L$(ICLIBHOME)
CCFLAGS=-Wall -DLINUX -D_GNU_SOURCE -D_REENTRANT -g
LDFLAGS=-g

THREADLIBS=-lpthread
CCLIB=$(ICLIBPATH) -locci $(THREADLIBS) -lclntsh 

CCINCLUDES = -I$(ICINCHOME)include

REMOVE=rm -rf

.cpp.o:
	$(CC) -c -I$(ICINCHOME)include $(CCFLAGS) $<

$(BIN): $(BIN).o
	$(CC) -o $(BIN) $(LDFLAGS) $(BIN).o $(CCLIB)

.PHONY: clean all

clean:
	$(REMOVE) orhn orhn.o *~

all: clean orhn
