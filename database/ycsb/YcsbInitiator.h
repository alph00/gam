#ifndef __YCSB_INITIATOR_H__
#define __YCSB_INITIATOR_H__

#include "BenchmarkInitiator.h"
#include "ColumnInfo.h"
#include "Meta.h"
#include "YcsbConstants.h"
#include "ycsb/YcsbParameter.h"
#include <cstddef>
#include <cstdlib>

namespace Database {
namespace YcsbBenchmark {
class YcsbInitiator : public BenchmarkInitiator {
public:
  YcsbInitiator(const size_t &thread_count, ClusterConfig *config)
      : BenchmarkInitiator(thread_count, config) {}
  ~YcsbInitiator() {}

protected:
  virtual void RegisterTables(const GAddr &storage_addr,
                              const std::vector<RecordSchema *> &schemas) {
    StorageManager storage_manager;
    storage_manager.RegisterTables(schemas, default_gallocator);
    storage_manager.Serialize(storage_addr, default_gallocator);
  }

  virtual void RegisterSchemas(std::vector<RecordSchema *> &schemas) {
    schemas.resize(kTableCount, nullptr);
    InitMainTableSchema(schemas[MAIN_TABLE_ID], default_gallocator);
  }

public:
  static void InitMainTableSchema(RecordSchema *&schema, GAlloc *gallocator) {
    std::vector<ColumnInfo *> columns;
    for (auto i = 0; i < 10; ++i) {
      char *name = (char *)malloc(3);
      name[0] = 'F';
      name[1] = '0' + i;
      name[2] = '\0';
      if (i == 0)
        columns.push_back(new ColumnInfo(name, ValueType::INT32));
      else {
        columns.push_back(
            new ColumnInfo(name, ValueType::VARCHAR,
                           static_cast<size_t>(YCSB_TABLE_F_STRING_SIZE)));
      }
    }
    columns.push_back(new ColumnInfo("meta", ValueType::META));

    schema = new RecordSchema(MAIN_TABLE_ID); // table_id会出问题吗？
    schema->InsertColumns(columns);
    size_t col_ids[] = {0};
    schema->SetPrimaryColumns(col_ids, 1);
    schema->SetPartitionColumns(col_ids, 1); // 这是在做什么?
  }
};
} // namespace YcsbBenchmark
} // namespace Database

#endif
