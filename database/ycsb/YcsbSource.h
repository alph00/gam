#ifndef __DATABASE_YCSB_SOURCE_H__
#define __DATABASE_YCSB_SOURCE_H__

#include "BenchmarkSource.h"
#include "YcsbRandomGenerator.h"
#include "YcsbTxnParams.h"
#include <ctime>

namespace Database {
namespace YcsbBenchmark {
class YcsbSource : public BenchmarkSource {
public:
  YcsbSource(IORedirector *redirector, size_t num_txn, size_t source_type,
             size_t thread_count, size_t dist_ratio = 0, size_t node_id = 0)
      : BenchmarkSource(redirector, num_txn, source_type, thread_count,
                        dist_ratio) {
    node_id_ = node_id;
  }

private:
  size_t node_id_;

  virtual void StartGeneration() {
    // srand(time(nullptr));
    srand(node_id_ * time(nullptr));

    double total = 0;

    if (source_type_ == RANDOM_SOURCE) {
      ParamBatch *tuples = new ParamBatch(gParamBatchSize);
      for (size_t i = 0; i < num_txn_; ++i) {
        YcsbParam *param = NULL;
        param = new YcsbParam();
        tuples->push_back(param);
        if ((i + 1) % gParamBatchSize == 0) {
          redirector_ptr_->PushParameterBatch(tuples);
          tuples = new ParamBatch(gParamBatchSize);
        }
      }
      if (tuples->size() != 0) {
        redirector_ptr_->PushParameterBatch(tuples);
      }
    } else if (source_type_ == PARTITION_SOURCE) {
      // to do
      ParamBatch *tuples = new ParamBatch(gParamBatchSize);
      for (size_t i = 0; i < num_txn_; ++i) {
        YcsbParam *param = NULL;
        param = new YcsbParam();
        tuples->push_back(param);
        if ((i + 1) % gParamBatchSize == 0) {
          redirector_ptr_->PushParameterBatch(tuples);
          tuples = new ParamBatch(gParamBatchSize);
        }
      }
      if (tuples->size() != 0) {
        redirector_ptr_->PushParameterBatch(tuples);
      }
    }
  }
};
} // namespace YcsbBenchmark
} // namespace Database

#endif
