#ifndef __DATABASE_YCSB_PROCEDURE_H__
#define __DATABASE_YCSB_PROCEDURE_H__

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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <iostream>
#include <string>

namespace Database {
namespace YcsbBenchmark {

class YcsbProcedure : public StoredProcedure {
public:
  YcsbProcedure() { context_.txn_type_ = YCSB; }
  virtual ~YcsbProcedure() {}
  virtual bool Execute(TxnParam *param, CharArray &ret) {
    epicLog(LOG_DEBUG, "thread_id=%u,start ycsb", thread_id_);
    YcsbParam *ycsb_param = static_cast<YcsbParam *>(param);
    GAddr kw[ycsb_param->Txnlength + 10];
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
      key = YcsbRandomGenerator::GenerateInteger(0, YCSB_TABLE_LENGTH - 1);
#endif
      { // 避免两次访问同一个key
        // BEGIN_PHASE_MEASURE(thread_id_, INDEX_READ);
        // GAddr data_addr =
        //     transaction_manager_->storage_manager_->tables_[MAIN_TABLE_ID]
        //         ->SearchRecord(key, gallocators[thread_id_], thread_id_);
        // END_PHASE_MEASURE(thread_id_, INDEX_READ);
        // if (data_addr == Gnullptr) {
        //   assert(0);
        // }
        bool flag = false;
        // for (auto i = 0; i < kwcnt; ++i) {
        //   if (kw[i] <= 512 * 3) {
        //     assert(0);
        //   }
        //   if (data_addr <= kw[i] + 512 * 3 && data_addr >= kw[i] - 512 * 3) {
        //     flag = true;
        //     break;
        //   }
        // }
        if (flag) {
        } else {
          // kw[kwcnt] = data_addr;
          keys[kwcnt] = key;
          kwcnt++;
        }
      }
    }
    // sort
    // sort(keys, keys + ycsb_param->Txnlength);
    // execute
    for (auto j = 0; j < ycsb_param->Txnlength; ++j) {
      double x = YcsbRandomGenerator::GenerateInteger(1, 100) * 1.0 / 100;
      if (x < ycsb_param->getratio) { // get
        Record *record = nullptr;
        DB_QUERY(
            SearchRecord(&context_, MAIN_TABLE_ID, keys[j], record, READ_ONLY));
        for (auto i = 1; i < record->GetSchema()->GetColumnCount() - 1; ++i) {
          char field[record->GetSchema()->GetColumnSize(i)];
          record->GetColumn(i, field);
        }
      } else if (x >= ycsb_param->getratio &&
                 x < ycsb_param->getratio + ycsb_param->updateratio) { // update
        Record *record = nullptr;
        DB_QUERY(SearchRecord(&context_, MAIN_TABLE_ID, keys[j], record,
                              READ_WRITE));
        for (auto i = 1; i < record->GetSchema()->GetColumnCount() - 1; ++i) {
          char field[record->GetSchema()->GetColumnSize(i)];
          record->GetColumn(i, field);

          std::string data_s = "aaaaaaaaa";
          char *data_c = (char *)malloc(record->GetSchema()->GetColumnSize(i));
          memcpy(data_c, data_s.c_str(), record->GetSchema()->GetColumnSize(i));

          record->SetColumn(i, data_c);
        }
      } else {
        // to do for insert
      }
    }
    // for (auto i = 0; i < kwcnt; ++i) {
    //   for (auto j = i + 1; j < kwcnt; ++j) {
    //     if (kw[j] > kw[i]) {
    //       if (kw[i] - kw[j] <= 521)
    //         printf(" %zu-%lx-%lx \n", thread_id_, kw[i], kw[j]);
    //     } else {
    //       if (kw[j] - kw[i] <= 521)
    //         printf(" %zu-%lx-%lx \n", thread_id_, kw[i], kw[j]);
    //     }
    //   }
    // }

    return transaction_manager_->CommitTransaction(&context_, param, ret);
  }
};

} // namespace YcsbBenchmark
} // namespace Database
#endif
