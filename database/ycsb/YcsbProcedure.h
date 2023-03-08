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
        // execute
        for (auto j = 0; j < ycsb_param->Txnlength; ++j) {
            if (ycsb_param->op[j].type == YcsbGet) {  // get
                Record *record = nullptr;
                DB_QUERY(SearchRecord(&context_, MAIN_TABLE_ID,
                                      ycsb_param->op[j].key, record,
                                      READ_ONLY));
                for (auto i = 1; i < record->GetSchema()->GetColumnCount() - 1;
                     ++i) {
                    char field[record->GetSchema()->GetColumnSize(i)];
                    record->GetColumn(i, field);
                }
            } else if (ycsb_param->op[j].type == YcsbUpdate) {  // update
                Record *record = nullptr;
                DB_QUERY(SearchRecord(&context_, MAIN_TABLE_ID,
                                      ycsb_param->op[j].key, record,
                                      READ_WRITE));
                for (auto i = 1; i < record->GetSchema()->GetColumnCount() - 1;
                     ++i) {
                    char field[record->GetSchema()->GetColumnSize(i)];
                    record->GetColumn(i, field);
                    record->SetColumn(i, ycsb_param->op[j].wq[i].second);
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
