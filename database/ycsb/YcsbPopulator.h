#ifndef __DATABASE_TPCC_POPULATOR_H__
#define __DATABASE_TPCC_POPULATOR_H__

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "BenchmarkArguments.h"
#include "BenchmarkPopulator.h"
#include "ClusterConfig.h"
#include "ClusterHelper.h"
#include "Meta.h"
#include "TableRecord.h"
#include "YcsbParameter.h"
#include "YcsbRandomGenerator.h"
#include "YcsbRecords.h"
#include "gallocator.h"
#include "ycsb/YcsbConstants.h"

namespace Database {
namespace YcsbBenchmark {
class YcsbPopulator : public BenchmarkPopulator {
   public:
    YcsbPopulator(StorageManager *storage_manager, ClusterConfig *config)
        : BenchmarkPopulator(storage_manager), config__(config) {}

    virtual void StartPopulate() {
        YcsbRecord *ycsbrecord = new YcsbRecord();
        Record *MainTable_item_touple_buf =
            new Record(storage_manager_->tables_[MAIN_TABLE_ID]->GetSchema());
        TableRecord *MainTable_record_buf =
            new TableRecord(MainTable_item_touple_buf);

        ServerInfo myhost = config__->GetMyHostInfo();
        // int cnt = (myhost.addr_[9] - '8' + 10) % 10;
        int cnt = config__->GetMyPartitionId();
        int num = config__->GetPartitionNum();

        if (enable_checkpoint_reload) {
            if (YCSB_POPULATE_NODE_TYPE == 1) {
                if (cnt == YCSB_POPULATE_NODE_ID) {
                    std::ifstream input_file(storage_manager_->filename_ +
                                                 std::to_string(MAIN_TABLE_ID),
                                             std::ifstream::binary);
                    assert(input_file.good() == true);

                    size_t record_size =
                        storage_manager_->tables_[MAIN_TABLE_ID]
                            ->GetSchemaSize();
                    input_file.seekg(0, std::ios::end);
                    size_t file_size = static_cast<size_t>(input_file.tellg());
                    input_file.seekg(0, std::ios::beg);
                    size_t file_pos = 0;
                    while (file_pos < file_size) {
                        char *entry = new char[record_size];
                        input_file.read(entry, record_size);
                        memcpy(&ycsbrecord->F0, entry, YCSB_TABLE_F_INT_SIZE);
                        for (auto i = 0; i < YCSB_TABLE_ITEMCOUNT - 1; ++i) {
                            memcpy(ycsbrecord->F[i],
                                   entry + YCSB_TABLE_F_INT_SIZE +
                                       YCSB_TABLE_F_STRING_SIZE * i,
                                   YCSB_TABLE_F_STRING_SIZE);
                        }
                        InsertMainTableRecord(ycsbrecord, MainTable_record_buf,
                                              0);
                        file_pos += record_size;
                    }
                    input_file.close();
                }
            } else if (YCSB_POPULATE_NODE_TYPE == 0) {
                std::ifstream input_file(
                    storage_manager_->filename_ + std::to_string(MAIN_TABLE_ID),
                    std::ifstream::binary);
                assert(input_file.good() == true);

                size_t record_size =
                    storage_manager_->tables_[MAIN_TABLE_ID]->GetSchemaSize();
                input_file.seekg(0, std::ios::end);
                size_t file_size = static_cast<size_t>(input_file.tellg());
                input_file.seekg(0, std::ios::beg);
                size_t file_pos = 0;
                size_t record_cnt = 0;
                while (file_pos < file_size) {
                    char *entry = new char[record_size];
                    input_file.read(entry, record_size);
                    memcpy(&ycsbrecord->F0, entry, YCSB_TABLE_F_INT_SIZE);
                    for (auto i = 0; i < YCSB_TABLE_ITEMCOUNT - 1; ++i) {
                        memcpy(ycsbrecord->F[i],
                               entry + YCSB_TABLE_F_INT_SIZE +
                                   YCSB_TABLE_F_STRING_SIZE * i,
                               YCSB_TABLE_F_STRING_SIZE);
                    }
                    if (record_cnt < (YCSB_TABLE_LENTH / num) * (num)) {
                        if (cnt == record_cnt % num) {
                            InsertMainTableRecord(ycsbrecord,
                                                  MainTable_record_buf, 0);
                        }
                    } else {
                        if (cnt == num - 1) {
                            InsertMainTableRecord(ycsbrecord,
                                                  MainTable_record_buf, 0);
                        }
                    }
                    record_cnt += 1;
                    file_pos += record_size;
                }
                input_file.close();
            } else {
                std::cout << "YCSB_POPULATE_NODE_TYPE error" << endl;
            }
        } else {
            if (YCSB_POPULATE_NODE_TYPE == 1) {
                if (cnt == YCSB_POPULATE_NODE_ID) {
                    for (auto i = 0; i < YCSB_TABLE_LENTH; ++i) {
                        GenerateMainTableRecordRandom(i, ycsbrecord);
                        InsertMainTableRecord(ycsbrecord, MainTable_record_buf,
                                              0);
                    }
                }
            } else if (YCSB_POPULATE_NODE_TYPE == 0) {
                for (auto i = cnt; i < (YCSB_TABLE_LENTH / num) * (num);
                     i += num) {
                    GenerateMainTableRecordRandom(i, ycsbrecord);
                    InsertMainTableRecord(ycsbrecord, MainTable_record_buf, 0);
                }
                if (cnt == num - 1) {
                    for (auto i = (YCSB_TABLE_LENTH / num) * (num);
                         i < YCSB_TABLE_LENTH; ++i) {
                        GenerateMainTableRecordRandom(i, ycsbrecord);
                        InsertMainTableRecord(ycsbrecord, MainTable_record_buf,
                                              0);
                    }
                }
            } else {
                std::cout << "YCSB_POPULATE_NODE_TYPE error" << endl;
            }
        }

        for (size_t i = 0; i < kTableCount; ++i) {
            storage_manager_->tables_[i]->ReportTableSize();
        }

        delete MainTable_record_buf;
        MainTable_record_buf = nullptr;
    }

   private:
    ClusterConfig *config__;
    void GenerateMainTableRecordRandom(int id, YcsbRecord *record_buf) const {
        record_buf->F0 = id;
        for (auto i = 0; i < YCSB_TABLE_ITEMCOUNT - 1; ++i) {
            std::string data_s =
                YcsbRandomGenerator::GenerateAString(MIN_F1, MAX_F1);
            memcpy(record_buf->F[i], data_s.c_str(), data_s.length());
        }
    }
    void InsertMainTableRecord(YcsbRecord *record,
                               TableRecord *MainTable_record_buf,
                               size_t thread_id) {
        GAlloc *gallocator = gallocators[thread_id];
        Record *record_buf = MainTable_record_buf->record_;
        GAddr data_addr =
            gallocator->AlignedMalloc(MainTable_record_buf->GetSerializeSize());

        record_buf->SetColumn(0, &record->F0);

        for (auto i = 1; i < record_buf->GetSchema()->GetColumnCount() - 1;
             ++i) {
            record_buf->SetColumn(i, record->F[i - 1]);
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
