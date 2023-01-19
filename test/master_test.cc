// Copyright (c) 2018 The GAM Authors

#include "master.h"

#include "gallocator.h"
#include "settings.h"
#include "structure.h"
#include "worker.h"
#include "worker_handle.h"

int main() {
    Conf* conf = new Conf();
    GAllocFactory::SetConf(conf);
    Master* master = new Master(*conf);
    master->Join();
    return 0;
}
