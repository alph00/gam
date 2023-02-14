// Copyright (c) 2018 The GAM Authors

#ifndef INCLUDE_WORKER_HANDLE_H_
#define INCLUDE_WORKER_HANDLE_H_

#include <atomic>
#include <mutex>

#include "lockwrapper.h"
#include "worker.h"
#include "workrequest.h"

class WorkerHandle {
    boost::lockfree::queue<WorkRequest*>*
        wqueue;  // work queue used to communicate with worker
    Worker* worker;
    // app-side pipe fd
    int send_pipe[2];
    int recv_pipe[2];
    static LockWrapper lock;
#ifdef USE_PTHREAD_COND
    pthread_mutex_t cond_lock;
    pthread_cond_t cond;
#endif
#if !(defined(USE_PIPE_W_TO_H) && defined(USE_PIPE_H_TO_W))
    volatile int* notify_buf;
    int notify_buf_size;
#endif
   public:
    WorkerHandle(Worker* w);
    void RegisterThread();
    void DeRegisterThread();
    int SendRequest(WorkRequest* wr);
    inline int GetWorkerId() { return worker->GetWorkerId(); }
    int GetWorkersSize() { return worker->GetWorkersSize(); }
    inline void* GetLocal(GAddr addr) { return worker->ToLocal(addr); }

    // inline bool CheckVersionLocally(const GAddr base_addr, const Size count, const Size offset, uint64_t version) {
    //     return worker->ProcessLocalVersionCheck(base_addr, count, offset, version);
    // }

    void InitWorkerMemStatsForTest(Size total, Size free) {
        worker->InitMemStatsForTest(total, free);
    }

    void ReportCacheStatistics();
    void ResetCacheStatistics();

    ~WorkerHandle();
};

#endif /* INCLUDE_WORKER_HANDLE_H_ */
