#ifndef __DATABASE_YCSB_TXN_PARAMS_H__
#define __DATABASE_YCSB_TXN_PARAMS_H__

#include <string>

#include "BenchmarkArguments.h"
#include "CharArray.h"
#include "TxnParam.h"
#include "YcsbConstants.h"
#include "YcsbParameter.h"

namespace Database {
namespace YcsbBenchmark {

class YcsbParam : public TxnParam {
   public:
    YcsbParam() { type_ = YCSB; }
    virtual ~YcsbParam() {}
    int Txnlength = YCSB_TXN_LEHTH;
    double getratio = ycsb_get_ratio;
    double putratio = ycsb_put_ratio;
    double updateratio = ycsb_update_ratio;
};

}  // namespace YcsbBenchmark
}  // namespace Database

#endif
