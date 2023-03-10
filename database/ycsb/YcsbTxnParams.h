#ifndef __DATABASE_YCSB_TXN_PARAMS_H__
#define __DATABASE_YCSB_TXN_PARAMS_H__

#include <cstddef>
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

class YcsbParam : public TxnParam {
   public:
    virtual void Serialize(CharArray& serial_str) const {
        size_t SizeOfYcsbOp =
            (sizeof(int) + sizeof(Key) + sizeof(pair<uint64_t, char*>*));
        serial_str.Allocate(sizeof(int) + YCSB_TXN_LEHTH * SizeOfYcsbOp);
        serial_str.Memcpy(0, reinterpret_cast<const char*>(&Txnlength),
                          sizeof(int));
        for (auto i = 0; i < YCSB_TXN_LEHTH; ++i) {
            serial_str.Memcpy(sizeof(int) + i * SizeOfYcsbOp,
                              reinterpret_cast<const char*>(&op[i].type),
                              sizeof(int));
            serial_str.Memcpy(sizeof(int) + i * SizeOfYcsbOp + sizeof(int),
                              reinterpret_cast<const char*>(&op[i].key),
                              sizeof(Key));
            serial_str.Memcpy(
                sizeof(int) + i * SizeOfYcsbOp + sizeof(int) + sizeof(Key),
                reinterpret_cast<const char*>(&op[i].wq),
                sizeof(pair<uint64_t, char*>*));
        }
    }

    virtual void Deserialize(const CharArray& serial_str) {
        size_t SizeOfYcsbOp =
            (sizeof(int) + sizeof(Key) + sizeof(pair<uint64_t, char*>*));
        memcpy(reinterpret_cast<char*>(&Txnlength), serial_str.char_ptr_,
               sizeof(int));
        for (auto i = 0; i < YCSB_TXN_LEHTH; ++i) {
            memcpy(reinterpret_cast<char*>(&op[i].type),
                   serial_str.char_ptr_ + sizeof(int) + i * SizeOfYcsbOp,
                   sizeof(int));
            memcpy(reinterpret_cast<char*>(&op[i].key),
                   serial_str.char_ptr_ + sizeof(int) + i * SizeOfYcsbOp +
                       sizeof(int),
                   sizeof(Key));
            memcpy(reinterpret_cast<char*>(&op[i].wq),
                   serial_str.char_ptr_ + sizeof(int) + i * SizeOfYcsbOp +
                       sizeof(int) + sizeof(Key),
                   sizeof(pair<uint64_t, char*>*));
        }
    }

   public:
    YcsbParam() { type_ = YCSB; }
    virtual ~YcsbParam() {}
    int Txnlength = YCSB_TXN_LEHTH;
    // double getratio = ycsb_get_ratio;
    // double putratio = ycsb_put_ratio;
    // double updateratio = ycsb_update_ratio;
    YcsbOp op[YCSB_TXN_LEHTH];
};

}  // namespace YcsbBenchmark
}  // namespace Database

#endif
