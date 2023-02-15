#ifndef __DATABASE_TPCC_POPULATOR_H__
#define __DATABASE_TPCC_POPULATOR_H__

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "BenchmarkPopulator.h"
#include "ClusterConfig.h"
#include "ClusterHelper.h"
#include "Meta.h"
#include "TableRecord.h"
#include "YcsbParameter.h"
#include "YcsbRandomGenerator.h"
#include "gallocator.h"
#include "ycsb/YcsbConstants.h"

namespace Database {
namespace YcsbBenchmark {
class YcsbPopulator : public BenchmarkPopulator {
   public:
    YcsbPopulator(StorageManager *storage_manager, ClusterConfig *config)
        : BenchmarkPopulator(storage_manager), config__(config) {}

    virtual void StartPopulate() {
        Record *MainTable_item_touple_buf =
            new Record(storage_manager_->tables_[MAIN_TABLE_ID]->GetSchema());
        TableRecord *MainTable_record_buf =
            new TableRecord(MainTable_item_touple_buf);

        ServerInfo myhost = config__->GetMyHostInfo();
        // int cnt = (myhost.addr_[9] - '8' + 10) % 10;
        int cnt = config__->GetMyPartitionId();
        int num = config__->GetPartitionNum();

        if (YCSB_POPULATE_NODE_TYPE == 1) {
            if (cnt == YCSB_POPULATE_NODE_ID) {
                for (auto i = 0; i < YCSB_TABLE_LENTH; ++i) {
                    InsertMainTableRecord(i, MainTable_record_buf, 0);
                }
            }
        } else if (YCSB_POPULATE_NODE_TYPE == 0) {
            for (auto i = cnt; i < (YCSB_TABLE_LENTH / num) * (num); i += num) {
                InsertMainTableRecord(i, MainTable_record_buf, 0);
            }
            if (cnt == num - 1) {
                for (auto i = (YCSB_TABLE_LENTH / num) * (num);
                     i < YCSB_TABLE_LENTH; ++i) {
                    InsertMainTableRecord(i, MainTable_record_buf, 0);
                }
            }
        } else {
            std::cout << "YCSB_POPULATE_NODE_TYPE error" << endl;
        }

        for (size_t i = 0; i < kTableCount; ++i) {
            storage_manager_->tables_[i]->ReportTableSize();
        }

        delete MainTable_record_buf;
        MainTable_record_buf = nullptr;
    }

   private:
    ClusterConfig *config__;
    void InsertMainTableRecord(int id, TableRecord *MainTable_record_buf,
                               size_t thread_id) {
        GAlloc *gallocator = gallocators[thread_id];
        Record *record_buf = MainTable_record_buf->record_;
        GAddr data_addr =
            gallocator->AlignedMalloc(MainTable_record_buf->GetSerializeSize());
        {
            int *data_i =
                (int *)malloc(record_buf->GetSchema()->GetColumnSize(0));
            int t = id;
            memcpy(data_i, &t, record_buf->GetSchema()->GetColumnSize(0));
            record_buf->SetColumn(0, data_i);
        }
        for (auto i = 1; i < record_buf->GetSchema()->GetColumnCount() - 1;
             ++i) {
            std::string data_s =
                YcsbRandomGenerator::GenerateAString(MIN_F1, MAX_F1);
            char *data_c =
                (char *)malloc(record_buf->GetSchema()->GetColumnSize(i));
            memcpy(data_c, data_s.c_str(),
                   record_buf->GetSchema()->GetColumnSize(i));

            record_buf->SetColumn(i, data_c);
        }

        record_buf->SetVisible(true);
        record_buf->Serialize(data_addr, gallocator);
        int key_p;
        record_buf->GetColumn(0, &key_p);
        IndexKey key = (IndexKey)key_p;
        storage_manager_->tables_[MAIN_TABLE_ID]->InsertRecord(
            &key, 1, data_addr, gallocator, thread_id);
    }
};

}  // namespace YcsbBenchmark
}  // namespace Database
#endif
