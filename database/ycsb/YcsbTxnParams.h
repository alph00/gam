#ifndef __DATABASE_YCSB_TXN_PARAMS_H__
#define __DATABASE_YCSB_TXN_PARAMS_H__

#include <bits/stdint-uintn.h>

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
    pair<int, char*>* wq;
};

class YcsbParam : public TxnParam {
   public:
    virtual void Serialize(CharArray& serial_str) const {
        size_t SizeOfpair = (sizeof(int) + YCSB_TABLE_F_STRING_SIZE);
        size_t SizeOfwq = (YCSB_TABLE_ITEMCOUNT - 1) * SizeOfpair;
        size_t SizeOfYcsbOp = (sizeof(int) + sizeof(Key) + SizeOfwq);
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
            for (auto j = 1; j < YCSB_TABLE_ITEMCOUNT; ++j) {
                if (op[i].type == YcsbUpdate) {
                    serial_str.Memcpy(
                        sizeof(int) + i * SizeOfYcsbOp + sizeof(int) +
                            sizeof(Key) + (j - 1) * SizeOfpair,
                        reinterpret_cast<const char*>(&op[i].wq[j].first),
                        sizeof(int));
                    serial_str.Memcpy(
                        sizeof(int) + i * SizeOfYcsbOp + sizeof(int) +
                            sizeof(Key) + (j - 1) * SizeOfpair + sizeof(int),
                        op[i].wq[j].second, YCSB_TABLE_F_STRING_SIZE);
                }
            }
        }
    }

    virtual void Deserialize(const CharArray& serial_str) {
        size_t SizeOfpair = (sizeof(int) + YCSB_TABLE_F_STRING_SIZE);
        size_t SizeOfwq = (YCSB_TABLE_ITEMCOUNT - 1) * SizeOfpair;
        size_t SizeOfYcsbOp = (sizeof(int) + sizeof(Key) + SizeOfwq);
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
            if (op[i].type == YcsbUpdate) {
                op[i].wq = (pair<int, char*>*)malloc(YCSB_TABLE_ITEMCOUNT *
                                                     sizeof(pair<int, char*>));
                for (auto j = 1; j < YCSB_TABLE_ITEMCOUNT; ++j) {
                    memcpy(reinterpret_cast<char*>(&op[i].wq[j].first),
                           serial_str.char_ptr_ + sizeof(int) +
                               i * SizeOfYcsbOp + sizeof(int) + sizeof(Key) +
                               (j - 1) * SizeOfpair,
                           sizeof(int));
                    op[i].wq[j].second =
                        (char*)malloc(YCSB_TABLE_F_STRING_SIZE);
                    memcpy(op[i].wq[j].second,
                           serial_str.char_ptr_ + sizeof(int) +
                               i * SizeOfYcsbOp + sizeof(int) + sizeof(Key) +
                               (j - 1) * SizeOfpair + sizeof(int),
                           YCSB_TABLE_F_STRING_SIZE);
                }
            }
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
