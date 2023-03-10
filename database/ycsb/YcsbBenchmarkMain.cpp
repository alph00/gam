#include "BenchmarkArguments.h"
#include "ClusterHelper.h"
#include "ClusterSync.h"
#include "Meta.h"
// #include "TpccParams.h"
#include <iostream>

#include "../profiler/Profilers.h"
#include "../txn/TimestampManager.h"
#include "YcsbConstants.h"
#include "YcsbExecutor.h"
#include "YcsbInitiator.h"
#include "YcsbPopulator.h"
#include "YcsbSource.h"
#include "ycsb/zipf.h"

using namespace Database::YcsbBenchmark;
using namespace Database;

void ExchPerfStatistics(ClusterConfig *config, ClusterSync *synchronizer,
                        PerfStatistics *s);

int main(int argc, char *argv[]) {
    ArgumentsParser(argc, argv);  // to do

    std::string my_host_name = ClusterHelper::GetLocalHostName();
    ClusterConfig config(my_host_name, port, config_filename);
    ClusterSync synchronizer(&config);
    // FillScaleParams(config);
    // PrintScaleParams();

    YcsbInitiator initiator(gThreadCount, &config);
    // init ycsb parameter:zipian
    Zipf::globalZipf().init(YCSB_TABLE_LENTH, ycsb_theta);

    // initialize GAM storage layer
    initiator.InitGAllocator();
    synchronizer.Fence();

    // initialize benchmark data
    GAddr storage_addr = initiator.InitStorage();
    synchronizer.MasterBroadcast<GAddr>(&storage_addr);
    std::cout << "storage_addr=" << storage_addr << std::endl;
    StorageManager storage_manager(table_data_filename);
    storage_manager.Deserialize(storage_addr, default_gallocator);

    GAddr epoch_addr = Gnullptr;
#if defined(OCC) || (defined(SILO) && !defined(USE_DECENTRALIZED_TID))
    epoch_addr = initiator.InitEpoch();
    // synchronizer.MasterBroadcast<GAddr>(&epoch_addr,
    // Database::SyncType::EPOCH);
    synchronizer.MasterBroadcast<GAddr>(&epoch_addr);
    std::cout << "epoch_addr=" << epoch_addr << std::endl;
#endif

    GAddr monotone_ts_addr = initiator.InitMonotoneTimestamp();
    // synchronizer.MasterBroadcast<GAddr>(&monotone_ts_addr,
    // Database::SyncType::MONOTONE_TIMESTAMP);
    synchronizer.MasterBroadcast<GAddr>(&monotone_ts_addr);
    std::cout << "monotone_ts_addr=" << monotone_ts_addr << std::endl;

    TimestampManager ts_manager(epoch_addr, monotone_ts_addr, config.IsMaster(),
                                epoch_gallocator);
    synchronizer.Fence();

    // populate database
    INIT_PROFILERS;
    YcsbPopulator populator(&storage_manager, &config);
    populator.Start();
    REPORT_PROFILERS;
    synchronizer.Fence();
    if (enable_checkpoint_save) {
        storage_manager.SaveCheckpoint(
            gallocators[0]);  // to qfs 这里的gallocators应该用这个吗?
        synchronizer.Fence();
    }

    // generate workload
    IORedirector redirector(gThreadCount);
    size_t access_pattern = 0;
    YcsbSource sourcer(&redirector, num_txn, SourceType::PARTITION_SOURCE,
                       gThreadCount, dist_ratio, config.GetMyPartitionId(),
                       &storage_manager, txn_data_filename);
    // TpccSource sourcer(&tpcc_scale_params, &redirector, num_txn,
    // SourceType::RANDOM_SOURCE, gThreadCount, dist_ratio);
    sourcer.Start();
    synchronizer.Fence();

    {
        // warm up
        INIT_PROFILERS;
        YcsbExecutor executor(&redirector, &storage_manager, &ts_manager,
                              gThreadCount);
        executor.Start();
        REPORT_PROFILERS;
    }
    synchronizer.Fence();

    {
        // run workload
        INIT_PROFILERS;
        YcsbExecutor executor(&redirector, &storage_manager, &ts_manager,
                              gThreadCount);
        executor.Start();
        REPORT_PROFILERS;
        ExchPerfStatistics(&config, &synchronizer,
                           &executor.GetPerfStatistics());
    }

    std::cout << "prepare to exit..." << std::endl;
    synchronizer.Fence();
    std::cout << "over.." << std::endl;
    return 0;
}

void ExchPerfStatistics(ClusterConfig *config, ClusterSync *synchronizer,
                        PerfStatistics *s) {
    PerfStatistics *stats = new PerfStatistics[config->GetPartitionNum()];
    synchronizer->MasterCollect<PerfStatistics>(s, stats);
    synchronizer->MasterBroadcast<PerfStatistics>(stats);
    for (size_t i = 0; i < config->GetPartitionNum(); ++i) {
        stats[i].Print();
        stats[0].Aggregate(stats[i]);
    }
    stats[0].PrintAgg();
    delete[] stats;
    stats = nullptr;
}
