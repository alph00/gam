#include <cstdlib>
#include <iostream>
#include <thread>

#include "BenchmarkArguments.h"
#include "ClusterHelper.h"
#include "ClusterSync.h"
#include "Meta.h"
#include "Profilers.h"
#include "TimestampManager.h"
#include "TpccConstants.h"
#include "TpccExecutor.h"
#include "TpccInitiator.h"
#include "TpccParams.h"
#include "TpccPopulator.h"
#include "TpccSource.h"

using namespace Database::TpccBenchmark;
using namespace Database;

void ExchPerfStatistics(ClusterConfig* config, ClusterSync* synchronizer,
                        PerfStatistics* s);

int main(int argc, char* argv[]) {
    ArgumentsParser(argc, argv);

    std::string my_host_name = ClusterHelper::GetLocalHostName();
    ClusterConfig config(my_host_name, port, config_filename);
    ClusterSync synchronizer(&config);
    FillScaleParams(config);
    PrintScaleParams();

    TpccInitiator initiator(gThreadCount, &config);
    // initialize GAM storage layer
    initiator.InitGAllocator();
    // 在Master和Workers之间同步, 方式为Master等待Workers写入某个kv(round 1),
    // 完成后自己写入某个kv(round 2), Workers等待Master写入kv完成(round 2).
    synchronizer.Fence();
    // initialize benchmark data
    // master初始化meta data, 写入gam; workers直接从gam读即可
    GAddr storage_addr = initiator.InitStorage();
    // synchronizer.MasterBroadcast<GAddr>(&storage_addr,
    // Database::SyncType::STORAGE_MANAGER);
    synchronizer.MasterBroadcast<GAddr>(&storage_addr);
    std::cout << "storage_addr=" << storage_addr << std::endl;
    StorageManager storage_manager;
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
    // INIT_PROFILE_TIME(gThreadCount);
    // if (enable_checkpoint_reload) {
    if (0) {
        INIT_PROFILERS;
        storage_manager.ReloadCheckpointTpcc();
        REPORT_PROFILERS;
    } else {
        INIT_PROFILERS;
        // 每个node负责自己所属的(start, end warehouse), 生成初始数据并写入gam
        TpccPopulator populator(&storage_manager, &tpcc_scale_params);
        populator.Start();
        // REPORT_PROFILE_TIME(gThreadCount);
        REPORT_PROFILERS;
    }
    synchronizer.Fence();

    // if (enable_checkpoint_save) {
    //     map<Key, int> OrderLineCnts;
    //     storage_manager.SaveCheckpointTpcc(
    //         gallocators[0],
    //         &OrderLineCnts);  // to qfs 这里的gallocators应该用这个吗?
    //     synchronizer.Fence();
    //     OrderLineCnts.clear();
    // }

    // generate workload
    IORedirector redirector(gThreadCount);
    size_t access_pattern = 0;
    TpccSource sourcer(&tpcc_scale_params, &redirector, num_txn,
                       SourceType::PARTITION_SOURCE, gThreadCount, dist_ratio,
                       config.GetMyPartitionId());
    // TpccSource sourcer(&tpcc_scale_params, &redirector, num_txn,
    // SourceType::RANDOM_SOURCE, gThreadCount, dist_ratio);
    sourcer.Start();
    synchronizer.Fence();

    // test multi-thread source reply correctness for checkpoint
    // for (auto p = 0; p < gThreadCount; ++p) {
    //     ofstream output_file_;
    //     output_file_.open(
    //         "txn_data_thread" + std::to_string(p) + std::string("save2"),
    //         std::ofstream::binary);
    //     std::vector<ParamBatch*>& execution_batches =
    //         *(redirector.GetParameterBatches(p));
    //     for (auto& tuples : execution_batches) {
    //         for (size_t i = 0; i < tuples->size(); ++i) {
    //             TxnParam* tuple = (tuples->get(i));
    //             CharArray param_chars;
    //             tuple->Serialize(param_chars);
    //             // write stored procedure type.
    //             size_t tuple_type = tuple->type_;
    //             output_file_.write((char*)(&tuple_type), sizeof(tuple_type));
    //             // write parameter size.
    //             output_file_.write((char*)(&param_chars.size_),
    //                                sizeof(param_chars.size_));
    //             // write parameters.
    //             output_file_.write(param_chars.char_ptr_, param_chars.size_);
    //             output_file_.flush();
    //             param_chars.Release();
    //         }
    //     }
    //     output_file_.close();
    // }

    {
        // warm up
        // INIT_PROFILE_TIME(gThreadCount);
        INIT_PROFILERS;
        TpccExecutor executor(&redirector, &storage_manager, &ts_manager,
                              gThreadCount);
        executor.Start();
        // REPORT_PROFILE_TIME(gThreadCount);
        REPORT_PROFILERS;
    }
    synchronizer.Fence();

    {
        ok = true;
        // run workload
        // INIT_PROFILE_TIME(gThreadCount);
        INIT_PROFILERS;
        TpccExecutor executor(&redirector, &storage_manager, &ts_manager,
                              gThreadCount);
        executor.Start();
        // REPORT_PROFILE_TIME(gThreadCount);
        REPORT_PROFILERS;
        ExchPerfStatistics(&config, &synchronizer,
                           &executor.GetPerfStatistics());
    }
    ok = false;

    std::cout << "prepare to exit..." << std::endl;
    synchronizer.Fence();
    std::cout << "over.." << std::endl;
    return 0;
}

void ExchPerfStatistics(ClusterConfig* config, ClusterSync* synchronizer,
                        PerfStatistics* s) {
    PerfStatistics* stats = new PerfStatistics[config->GetPartitionNum()];
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
