#ifndef _DATABASE_YCSB_RECORDS_H__
#define _DATABASE_YCSB_RECORDS_H__

#include <cstdint>
#include <sstream>
#include <string>

#include "ycsb/YcsbParameter.h"

namespace Database {
namespace YcsbBenchmark {

/******************** record **********************/
struct YcsbRecord {
    int32_t F0;
    char F[YCSB_TABLE_ITEMCOUNT - 1][YCSB_TABLE_F_STRING_SIZE];
    // std::string F1;
    // std::string F2;
    // std::string F3;
    // std::string F4;
    // std::string F5;
    // std::string F6;
    // std::string F7;
    // std::string F8;
    // std::string F9;
};

#define YCSB_RECORD_COPY(A, cnt, B) A->F##cnt = B;
// static std::string ItemToString(ItemRecord &item) {
//     std::stringstream item_str;
//     item_str << item.i_id_ << ";";
//     item_str << item.i_im_id_ << ";";
//     item_str << item.i_name_ << ";";
//     item_str << item.i_price_ << ";";
//     item_str << item.i_data_;
//     return item_str.str();
// }

}  // namespace YcsbBenchmark
}  // namespace Database

#endif
