#ifndef __DATABASE_YCSB_SOURCE_H__
#define __DATABASE_YCSB_SOURCE_H__

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <ios>
#include <string>
#include <utility>

#include "BenchmarkArguments.h"
#include "BenchmarkSource.h"
#include "Record.h"
#include "RecordSchema.h"
#include "StorageManager.h"
#include "YcsbRandomGenerator.h"
#include "YcsbTxnParams.h"
#include "ycsb/YcsbConstants.h"
#include "ycsb/YcsbParameter.h"
#include "ycsb/zipf.h"

namespace Database {
namespace YcsbBenchmark {
class YcsbSource : public BenchmarkSource {
   public:
    YcsbSource(IORedirector *redirector, size_t num_txn, size_t source_type,
               size_t thread_count, size_t dist_ratio = 0, size_t node_id = 0,
               StorageManager *storage_manager = nullptr,
               std::string &source_filename = txn_data_filename)
        : BenchmarkSource(redirector, num_txn, source_type, thread_count,
                          dist_ratio, source_filename) {
        node_id_ = node_id;
        storage_manager_ = storage_manager;
    }

   private:
    size_t node_id_;
    StorageManager *storage_manager_;

    virtual void StartGeneration() {
        // srand(time(nullptr));
        srand(node_id_ * time(nullptr));
        // if (access("./text0.txt", 0) == 0)
        // {
        //     remove("./text0.txt");
        // }

        if (source_type_ == RANDOM_SOURCE) {
            ParamBatch *tuples = new ParamBatch(gParamBatchSize);
            for (size_t i = 0; i < num_txn_; ++i) {
                YcsbParam *param = NULL;
                param = GenerateYcsbParam();
                tuples->push_back(param);
                if ((i + 1) % gParamBatchSize == 0) {
                    DumpToDisk(tuples);
                    redirector_ptr_->PushParameterBatch(tuples);
                    tuples = new ParamBatch(gParamBatchSize);
                }
            }
            if (tuples->size() != 0) {
                DumpToDisk(tuples);
                redirector_ptr_->PushParameterBatch(tuples);
            }
        } else if (source_type_ == PARTITION_SOURCE) {
            // to do
            ParamBatch *tuples = new ParamBatch(gParamBatchSize);
            for (size_t i = 0; i < num_txn_; ++i) {
                YcsbParam *param = NULL;
                param = GenerateYcsbParam();
                tuples->push_back(param);
                if ((i + 1) % gParamBatchSize == 0) {
                    DumpToDisk(tuples);
                    redirector_ptr_->PushParameterBatch(tuples);
                    tuples = new ParamBatch(gParamBatchSize);
                }
            }
            if (tuples->size() != 0) {
                DumpToDisk(tuples);
                redirector_ptr_->PushParameterBatch(tuples);
            }
        }
    }

    YcsbParam *GenerateYcsbParam() const {
        // srand(node_id_ * time(nullptr));

        YcsbParam *param = new YcsbParam();
        //
        int kwcnt = 0;
        // gen key
        Key keys[10 + param->Txnlength];
        while (kwcnt < param->Txnlength) {
            Key key;
            // zipfian for key
#ifdef ZIPFIAN
            key = Zipf::globalZipf().value(
                YcsbRandomGenerator::GenerateFixedPointRightOpen(6, 0, 1));
#elif defined UNIFORM
            key =
                YcsbRandomGenerator::GenerateInteger(0, YCSB_TABLE_LENGTH - 1);
#endif
            {  // 避免两次访问同一个key
                bool flag = false;
                for (auto i = 0; i < kwcnt; ++i) {
                    if (key == keys[i]) {
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    keys[kwcnt] = key;
                    kwcnt++;
                }
            }
        }
        // sort
        // sort(keys, keys + ycsb_param->Txnlength);
        for (auto j = 0; j < param->Txnlength; ++j) {
            double x = YcsbRandomGenerator::GenerateInteger(1, 100) * 1.0 / 100;
            if (x < ycsb_get_ratio) {  // get
                param->op[j].type = YcsbGet;
                param->op[j].key = keys[j];
            } else if (x >= ycsb_get_ratio &&
                       x <= ycsb_get_ratio + ycsb_update_ratio) {  // update
                param->op[j].type = YcsbUpdate;
                param->op[j].key = keys[j];
                // wq
                RecordSchema *schema =
                    storage_manager_->tables_[MAIN_TABLE_ID]->GetSchema();
                std::size_t ValueFieldNum = schema->GetColumnCount() - 1;
                param->op[j].wq = (pair<int, char *> *)malloc(
                    ValueFieldNum * sizeof(pair<int, char *>));
                for (auto i = 1; i < ValueFieldNum; ++i) {
                    std::string data_s =
                        YcsbRandomGenerator::GenerateAString(MIN_F1, MAX_F1);
                    char *data_c = (char *)malloc(schema->GetColumnSize(i));
                    memcpy(data_c, data_s.c_str(), schema->GetColumnSize(i));
                    param->op[j].wq[i] = make_pair(i, data_c);
                }
            } else {
                std::cout << "to do: insert! " << endl;
                assert(0);
                // to do for insert
            }
        }
        // ofstream ofs;
        // ofs.open("text0.txt", ios::out | ios::app);
        // for (auto m = 0; m < param->Txnlength; ++m) {
        //     ofs << param->op[m].key << endl;
        // }
        // ofs.close();
        return param;
    }
};
}  // namespace YcsbBenchmark
}  // namespace Database

#endif
