// Copyright (c) 2018 The GAM Authors

#include <cstring>
#include <iostream>
#include <thread>

#include "gallocator.h"
#include "master.h"
#include "settings.h"
#include "structure.h"
#include "worker.h"
#include "worker_handle.h"
#include "workrequest.h"

#define USE_LOCK
#define USE_TRY_LOCK

WorkerHandle *wh1_1, *wh1_2, *wh2_1, *wh2_2, *wh3_1, *wh3_2;
GAddr local_ts1_1, local_ts1_2, local_ts2_1, local_ts2_2, local_ts3_1, local_ts3_2; 
GAddr ts;

int test_count = 1000;

// using wh1_1
void thread1_1() {
    epicLog(LOG_WARNING,
            "*****************start thread1_1**********************");
    uint64_t now_ts = 0;
    WorkRequest* wr = new WorkRequest();

    for (int i = 0; i < test_count; ++i) {
        wr->Reset();
        wr->op = GET_AND_ADVANCE_TS;
        wr->addr = ts;
        wr->size = sizeof(uint64_t);
        wr->ts_adder = 1;
        wr->local_ts_addr = local_ts1_1;
        wr->ptr = &now_ts;
        if (wh1_1->SendRequest(wr)) {
            epicLog(LOG_WARNING, "GetAndAdvanceTs failed");
            epicAssert(false);
        }
    }

    epicLog(LOG_WARNING, "end thread1_1");
    delete wr;
    sleep(2);
}

// using wh1_2
void thread1_2() {
    epicLog(LOG_WARNING,
            "*****************start thread1_2**********************");
    uint64_t now_ts = 0;
    WorkRequest* wr = new WorkRequest();

    for (int i = 0; i < test_count; ++i) {
        wr->Reset();
        wr->op = GET_AND_ADVANCE_TS;
        wr->addr = ts;
        wr->size = sizeof(uint64_t);
        wr->ts_adder = 1;
        wr->local_ts_addr = local_ts1_2;
        wr->ptr = &now_ts;
        if (wh1_2->SendRequest(wr)) {
            epicLog(LOG_WARNING, "GetAndAdvanceTs failed");
            epicAssert(false);
        }
    }
    epicLog(LOG_WARNING, "end thread1_2");

    delete wr;
    sleep(2);
}

// using wh2_1
void thread2_1() {
    epicLog(LOG_WARNING,
            "*****************start thread2_1**********************");
    uint64_t now_ts = 0;
    WorkRequest* wr = new WorkRequest();

    for (int i = 0; i < test_count; ++i) {
        wr->Reset();
        wr->op = GET_AND_ADVANCE_TS;
        wr->addr = ts;
        wr->size = sizeof(uint64_t);
        wr->ts_adder = 1;
        wr->local_ts_addr = local_ts2_1;
        wr->ptr = &now_ts;
        if (wh2_1->SendRequest(wr)) {
            epicLog(LOG_WARNING, "GetAndAdvanceTs failed");
            epicAssert(false);
        }
    }

    epicLog(LOG_WARNING, "end thread2_1");

    delete wr;
    sleep(2);
}

// using wh2_2
void thread2_2() {
    epicLog(LOG_WARNING,
            "*****************start thread2_2**********************");
    uint64_t now_ts = 0;
    WorkRequest* wr = new WorkRequest();

    for (int i = 0; i < test_count; ++i) {
        wr->Reset();
        wr->op = GET_AND_ADVANCE_TS;
        wr->addr = ts;
        wr->size = sizeof(uint64_t);
        wr->ts_adder = 1;
        wr->local_ts_addr = local_ts2_2;
        wr->ptr = &now_ts;
        if (wh2_2->SendRequest(wr)) {
            epicLog(LOG_WARNING, "GetAndAdvanceTs failed");
            epicAssert(false);
        }
    }

    epicLog(LOG_WARNING, "end thread2_2");

    delete wr;
    sleep(2);
}

// using wh3_1
void thread3_1() {
    epicLog(LOG_WARNING,
            "*****************start thread3_1**********************");
    uint64_t now_ts = 0;
    WorkRequest* wr = new WorkRequest();

    for (int i = 0; i < test_count; ++i) {
        wr->Reset();
        wr->op = GET_AND_ADVANCE_TS;
        wr->addr = ts;
        wr->size = sizeof(uint64_t);
        wr->ts_adder = 1;
        wr->local_ts_addr = local_ts3_1;
        wr->ptr = &now_ts;
        if (wh3_1->SendRequest(wr)) {
            epicLog(LOG_WARNING, "GetAndAdvanceTs failed");
            epicAssert(false);
        }
    }
    epicLog(LOG_WARNING, "end thread3_1");
    delete wr;
    sleep(2);
}

// using wh3_2
void thread3_2() {
    epicLog(LOG_WARNING,
            "*****************start thread3_2**********************");
    uint64_t now_ts = 0;
    WorkRequest* wr = new WorkRequest();

    for (int i = 0; i < test_count; ++i) {
        wr->Reset();
        wr->op = GET_AND_ADVANCE_TS;
        wr->addr = ts;
        wr->size = sizeof(uint64_t);
        wr->ts_adder = 1;
        wr->local_ts_addr = local_ts3_2;
        wr->ptr = &now_ts;
        if (wh3_2->SendRequest(wr)) {
            epicLog(LOG_WARNING, "GetAndAdvanceTs failed");
            epicAssert(false);
        }
    }
    epicLog(LOG_WARNING, "end thread3_2");
    delete wr;
    sleep(2);
}

int main() {
    ibv_device** list = ibv_get_device_list(NULL);
    int i;

    // master
    Conf* conf = new Conf();
    conf->loglevel = LOG_WARNING;
    // conf->loglevel = LOG_DEBUG;
    GAllocFactory::SetConf(conf);
    Master* master = new Master(*conf);

    // worker1
    conf = new Conf();
    RdmaResource* res = new RdmaResource(list[0], false);
    Worker *worker1, *worker2, *worker3;
    worker1 = new Worker(*conf, res);
    wh1_1 = new WorkerHandle(worker1);
    wh1_2 = new WorkerHandle(worker1);

    // worker2
    conf = new Conf();
    res = new RdmaResource(list[0], false);
    conf->worker_port += 1;
    worker2 = new Worker(*conf, res);
    wh2_1 = new WorkerHandle(worker2);
    wh2_2 = new WorkerHandle(worker2);

    // worker3
    conf = new Conf();
    res = new RdmaResource(list[0], false);
    conf->worker_port += 2;
    worker3 = new Worker(*conf, res);
    wh3_1 = new WorkerHandle(worker3);
    wh3_2 = new WorkerHandle(worker3);

    sleep(1);

    WorkRequest wr{};

    // 1.allocate mem for global ts
    // since gam malloc would init mem to 0, we don't need to reinit.
    wr.Reset();
    wr.op = MALLOC;
    wr.size = sizeof(uint64_t);
    wr.flag |= ALIGNED;
    if (wh1_1->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
    }
    ts = wr.addr;
    epicLog(LOG_WARNING, "\n****allocated global_ts %ld at %lx*****\n", sizeof(uint64_t), ts);

    // 2.allocate local mem for local ts
    wr.Reset();
    wr.op = MALLOC;
    wr.size = sizeof(uint64_t);
    wr.flag |= ALIGNED;
    if (wh1_1->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
    }
    local_ts1_1 = wr.addr;
    epicLog(LOG_WARNING, "\n****allocated local_ts1_1 %ld at %lx*****\n", sizeof(uint64_t), local_ts1_1);

    wr.Reset();
    wr.op = MALLOC;
    wr.size = sizeof(uint64_t);
    wr.flag |= ALIGNED;
    if (wh1_2->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
    }
    local_ts1_2 = wr.addr;
    epicLog(LOG_WARNING, "\n****allocated local_ts1_2 %ld at %lx*****\n", sizeof(uint64_t), local_ts1_2);

    wr.Reset();
    wr.op = MALLOC;
    wr.size = sizeof(uint64_t);
    wr.flag |= ALIGNED;
    if (wh2_1->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
    }
    local_ts2_1 = wr.addr;
    epicLog(LOG_WARNING, "\n****allocated local_ts2_1 %ld at %lx*****\n", sizeof(uint64_t), local_ts2_1);

    wr.Reset();
    wr.op = MALLOC;
    wr.size = sizeof(uint64_t);
    wr.flag |= ALIGNED;
    if (wh2_2->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
    }
    local_ts2_2 = wr.addr;
    epicLog(LOG_WARNING, "\n****allocated local_ts2_2 %ld at %lx*****\n", sizeof(uint64_t), local_ts2_2);

    wr.Reset();
    wr.op = MALLOC;
    wr.size = sizeof(uint64_t);
    wr.flag |= ALIGNED;
    if (wh3_1->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
    }
    local_ts3_1 = wr.addr;
    epicLog(LOG_WARNING, "\n****allocated local_ts3_1 %ld at %lx*****\n", sizeof(uint64_t), local_ts3_1);

    wr.Reset();
    wr.op = MALLOC;
    wr.size = sizeof(uint64_t);
    wr.flag |= ALIGNED;
    if (wh3_2->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "send request failed");
    }
    local_ts3_2 = wr.addr;
    epicLog(LOG_WARNING, "\n****allocated local_ts3_2 %ld at %lx*****\n", sizeof(uint64_t), local_ts3_2);

    // 3.test
    sleep(1);

    thread* t1_1 = new thread(thread1_1);
    thread* t1_2 = new thread(thread1_2);
    thread* t2_1 = new thread(thread2_1);
    thread* t2_2 = new thread(thread2_2);
    thread* t3_1 = new thread(thread3_1);
    thread* t3_2 = new thread(thread3_2);

    t1_1->join();
    t1_2->join();
    t2_1->join();
    t2_2->join();
    t3_1->join();
    t3_2->join();

    uint64_t now_ts = 0;
    wr.Reset();
    wr.op = GET_AND_ADVANCE_TS;
    wr.addr = ts;
    wr.size = sizeof(uint64_t);
    wr.ts_adder = 1;
    wr.local_ts_addr = local_ts1_1;
    wr.ptr = &now_ts;
    if (wh1_1->SendRequest(&wr)) {
        epicLog(LOG_WARNING, "GetAndAdvanceTs failed");
    }
    if (now_ts == 6 * test_count + 1) {
        epicLog(LOG_WARNING, "atomic test succeeded");
    } else {
        epicLog(LOG_WARNING, "atomic test failed");
    }

    sleep(2);
    epicLog(LOG_WARNING, "test done");
    return 0;
}
