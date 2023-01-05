#ifndef __DATABASE_YCSB_TXN_PARAMS_H__
#define __DATABASE_YCSB_TXN_PARAMS_H__

#include "CharArray.h"
#include "TxnParam.h"
#include "YcsbConstants.h"
#include "YcsbParameter.h"
#include <string>

namespace Database {
namespace YcsbBenchmark {

class YcsbParam : public TxnParam {
public:
  YcsbParam() { type_ = YCSB; }
  virtual ~YcsbParam() {}
  int Txnlength = YCSB_TXN_LEHTH;
  double getratio = YCSB_GET_RATIO;
  double putratio = YCSB_PUT_RATIO;
  double updateratio = YCSB_UPDATE_RATIO;
};

} // namespace YcsbBenchmark
} // namespace Database

#endif
