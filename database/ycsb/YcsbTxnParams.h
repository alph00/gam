#ifndef __DATABASE_YCSB_TXN_PARAMS_H__
#define __DATABASE_YCSB_TXN_PARAMS_H__

#include <cstdint>
#include <queue>
#include <string>

#include "BenchmarkArguments.h"
#include "CharArray.h"
#include "TxnParam.h"
#include "YcsbConstants.h"
#include "YcsbParameter.h"
#include "structure.h"

namespace Database {
namespace YcsbBenchmark {

class YcsbOp {
   public:
    int type;
    Key key;
    pair<uint64_t, char*>* wq;
};
// class YcsbGetOp : YcsbOp {};
// class YcsbUpdateOp : YcsbOp {
//     pair<uint64_t, char*>** wq;
// };
class YcsbParam : public TxnParam {
   public:
    YcsbParam() { type_ = YCSB; }
    virtual ~YcsbParam() {}
    int Txnlength = YCSB_TXN_LEHTH;
    double getratio = ycsb_get_ratio;
    double putratio = ycsb_put_ratio;
    double updateratio = ycsb_update_ratio;
    YcsbOp op[YCSB_TXN_LEHTH];
};

}  // namespace YcsbBenchmark
}  // namespace Database

#endif
