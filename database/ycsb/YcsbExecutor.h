#ifndef __DATABASE_YCSB_EXECUTOR_H__
#define __DATABASE_YCSB_EXECUTOR_H__

#include "TransactionExecutor.h"
#include "YcsbProcedure.h"

namespace Database {
namespace YcsbBenchmark {
class YcsbExecutor : public TransactionExecutor {
public:
  YcsbExecutor(IORedirector *const redirector, StorageManager *storage_manager,
               TimestampManager *ts_manager, size_t thread_count_)
      : TransactionExecutor(redirector, storage_manager, ts_manager,
                            thread_count_) {}
  ~YcsbExecutor() {}

  virtual void PrepareProcedures() {
    registers_[TupleType::YCSB] = []() {
      YcsbProcedure *procedure = new YcsbProcedure();
      return procedure;
    };
  }
};
} // namespace YcsbBenchmark
} // namespace Database
#endif
