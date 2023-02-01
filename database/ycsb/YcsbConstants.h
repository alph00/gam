#ifndef __DATABASE_YCSB_CONSTANTS_H__
#define __DATABASE_YCSB_CONSTANTS_H__

#include <string>

#include "YcsbParameter.h"

namespace Database {
namespace YcsbBenchmark {
/********************procedure **********************/
enum TupleType : size_t { YCSB, kProcedureCount };

/******************** table **********************/
enum TableType : size_t { MAIN_TABLE_ID, kTableCount };

/******************** column **********************/
// const int MIN_F0 = YCSB_TABLE_F_INT_MIN;
// const int MAX_F0 = YCSB_TABLE_F_INT_MAX; //?

const int MIN_F1 = YCSB_TABLE_F_STRING_SIZE;  //?
const int MAX_F1 = YCSB_TABLE_F_STRING_SIZE;  //?
}  // namespace YcsbBenchmark
}  // namespace Database

#endif
