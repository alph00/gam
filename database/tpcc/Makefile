CPP = g++-7 -std=c++11
GAM_INCLUDE = -I../../include -I../ -I../../lib/libcuckoo/src -I../../lib/libcuckoo/cityhash-1.1.1/src
DB_INCLUDE = -I.. -I../bench -I../storage -I../txn -I../utils -I../content -I../profiler
INCLUDE = $(GAM_INCLUDE) $(DB_INCLUDE)
LIBS = ../../src/libgalloc.a -libverbs -lboost_filesystem -lboost_system -lboost_date_time -lboost_thread ../../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a -lpthread
CFLAGS += -O3 -DSILO -DUSE_DECENTRALIZED_TID -DBACKOFF #-DPROFILE_CC_TS_ALLOC -DPROFILE_PHASE -DPROFILE_EXECUTION -DGAM_OPERATION_PROFILE -DPROFILE_CC_ABORT_TIME #-DSCALABLE_TIMESTAMP  #-DPROFILE

all: tpcc
build: tpcc

PROFILE_SRC = ../profiler/PhaseProfiler.cpp ../profiler/IndexTimeProfiler.cpp ../profiler/FlowExecutionProfiler.cpp ../profiler/ExecutionProfiler.cpp ../profiler/CCWaitTimeProfiler.cpp ../profiler/CCWaitCountProfiler.cpp ../profiler/CCTsAllocTimeProfiler.cpp ../profiler/CCMemAllocTimeProfiler.cpp ../profiler/CCExecutionTimeProfiler.cpp ../profiler/CCExecutionCountProfiler.cpp ../profiler/CCAbortTimeProfiler.cpp ../profiler/CCAbortCountProfiler.cpp ../profiler/GamOperationProfiler.cpp
PROFILE_OBJ = ../profiler/PhaseProfiler.o ../profiler/IndexTimeProfiler.o ../profiler/FlowExecutionProfiler.o ../profiler/ExecutionProfiler.o ../profiler/CCWaitTimeProfiler.o ../profiler/CCWaitCountProfiler.o ../profiler/CCTsAllocTimeProfiler.o ../profiler/CCMemAllocTimeProfiler.o ../profiler/CCExecutionTimeProfiler.o ../profiler/CCExecutionCountProfiler.o ../profiler/CCAbortTimeProfiler.o ../profiler/CCAbortCountProfiler.o ../profiler/GamOperationProfiler.o

TXN_SRC = ../txn/TransactionManager2PL.cpp ../txn/TransactionManagerLockWait.cpp ../txn/TransactionManagerST.cpp ../txn/TransactionManagerOcc.cpp ../txn/TransactionManagerSilo.cpp
TXN_OBJ = ../txn/TransactionManager2PL.o ../txn/TransactionManagerLockWait.o ../txn/TransactionManagerST.o ../txn/TransactionManagerOcc.o ../txn/TransactionManagerSilo.o

SRC = TpccBenchmarkMain.cpp TpccRandomGenerator.cpp TpccParams.cpp ../Meta.cpp ${TXN_SRC} ${PROFILE_SRC} #../utils/Profiler.cpp
OBJ = TpccBenchmarkMain.o TpccRandomGenerator.o TpccParams.o ../Meta.o ${TXN_OBJ} ${PROFILE_OBJ} #../utils/Profiler.o 

$(OBJ): %.o : %.cpp
	$(CPP) $< $(CFLAGS) $(INCLUDE) -g -c -o $@

tpcc: $(OBJ)
	$(CPP) $(CFLAGS) $(INCLUDE) -g -o $@ $^ $(LIBS) 

clean:
	rm -rf tpcc *.o ../*.o ../bench/*.o ../storage/*.o ../txn/*.o ../utils/*.o
