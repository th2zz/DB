##############################################################
#               CMake Project Wrapper Makefile               #
############################################################## 
CC = g++
CFLAGS = -std=c++0x -Wall -g
OBJ = src/obj
LIB = src/lib

RHEL_VER := $(shell uname -r | grep -o -E '(el5|el6)')
ifeq ($(RHEL_VER), el5)
  PATH     := /s/gcc-4.6.1/bin:$(PATH)
endif
ifeq ($(RHEL_VER), el6)
  PATH     := /s/gcc-4.6.2/bin:$(PATH)
endif
export PATH

all: $(LIB)/bufmgr.a $(OBJ)/filescan.o $(OBJ)/main.o $(OBJ)/btree.o
	cd src;\
	rm -r ../relA*;\
	$(CC) $(CFLAGS) -I. obj/filescan.o obj/main.o obj/btree.o lib/bufmgr.a lib/exceptions.a -o badgerdb_main

$(LIB)/bufmgr.a: $(LIB)/exceptions.a src/buffer.* src/file.* src/page.* src/bufHashTbl.*
	cd $(OBJ)/;\
	$(CC) $(CFLAGS) -I.. -c ../buffer.cpp ../file.cpp ../page.cpp ../bufHashTbl.cpp;\
	ar cq ../lib/bufmgr.a buffer.o file.o page.o bufHashTbl.o

$(LIB)/exceptions.a: src/exceptions/*
	cd $(OBJ)/exceptions;\
	$(CC) $(CFLAGS) -c -I../../ ../../exceptions/*.cpp;\
	ar cq ../../lib/exceptions.a *.o

$(OBJ)/filescan.o: src/filescan.*
	cd $(OBJ)/;\
	$(CC) $(CFLAGS) -c -I../ ../filescan.cpp

$(OBJ)/main.o: src/main.cpp
	cd $(OBJ)/;\
	$(CC) $(CFLAGS) -c -I../ ../main.cpp

$(OBJ)/btree.o: src/btree.*
	cd $(OBJ)/;\
	$(CC) $(CFLAGS) -c -I../ ../btree.cpp

clean:
	rm -rf $(OBJ)/exceptions/*.o;\
	rm -rf $(OBJ)/*.o;\
	rm -rf $(LIB)/*;\
	rm -rf src/exceptions/*.o;\
	rm -f src/badgerdb_main

doc:
	doxygen Doxyfile
