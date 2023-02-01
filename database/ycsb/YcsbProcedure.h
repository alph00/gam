#ifndef __DATABASE_YCSB_PROCEDURE_H__
#define __DATABASE_YCSB_PROCEDURE_H__

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <iostream>
#include <string>

#include "Meta.h"
#include "Record.h"
#include "StoredProcedure.h"
#include "TxnParam.h"
#include "YcsbRandomGenerator.h"
#include "YcsbTxnParams.h"
#include "log.h"
#include "structure.h"
#include "ycsb/YcsbConstants.h"
#include "ycsb/YcsbParameter.h"
#include "ycsb/zipf.h"

namespace Database {
namespace YcsbBenchmark {

class YcsbProcedure : public StoredProcedure {
   public:
    YcsbProcedure() { context_.txn_type_ = YCSB; }
    virtual ~YcsbProcedure() {}
    virtual bool Execute(TxnParam *param, CharArray &ret) {
        epicLog(LOG_DEBUG, "thread_id=%u,start ycsb", thread_id_);
        YcsbParam *ycsb_param = static_cast<YcsbParam *>(param);
        int kwcnt = 0;

        // gen key
        Key keys[10 + ycsb_param->Txnlength];
        while (kwcnt < ycsb_param->Txnlength) {
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
        sort(keys, keys + ycsb_param->Txnlength);
        // execute
        for (auto j = 0; j < ycsb_param->Txnlength; ++j) {
            double x = YcsbRandomGenerator::GenerateInteger(1, 100) * 1.0 / 100;
            if (x < ycsb_param->getratio) {  // get
                Record *record = nullptr;
                DB_QUERY(SearchRecord(&context_, MAIN_TABLE_ID, keys[j], record,
                                      READ_ONLY));
                for (auto i = 1; i < record->GetSchema()->GetColumnCount() - 1;
                     ++i) {
                    char field[record->GetSchema()->GetColumnSize(i)];
                    record->GetColumn(i, field);
                }
            } else if (x >= ycsb_param->getratio &&
                       x < ycsb_param->getratio +
                               ycsb_param->updateratio) {  // update
                Record *record = nullptr;
                DB_QUERY(SearchRecord(&context_, MAIN_TABLE_ID, keys[j], record,
                                      READ_WRITE));
                for (auto i = 1; i < record->GetSchema()->GetColumnCount() - 1;
                     ++i) {
                    char field[record->GetSchema()->GetColumnSize(i)];
                    record->GetColumn(i, field);

                    std::string data_s = "aaaaaaaaa";
                    char *data_c =
                        (char *)malloc(record->GetSchema()->GetColumnSize(i));
                    memcpy(data_c, data_s.c_str(),
                           record->GetSchema()->GetColumnSize(i));

                    record->SetColumn(i, data_c);
                }
            } else {
                // to do for insert
            }
        }
        return transaction_manager_->CommitTransaction(&context_, param, ret);
    }
};

}  // namespace YcsbBenchmark
}  // namespace Database
#endif
