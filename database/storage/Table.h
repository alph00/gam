#ifndef __DATABASE_STORAGE_TABLE_H__
#define __DATABASE_STORAGE_TABLE_H__

#include <cstdlib>
#include <cstring>
#include <iostream>

// #include "Record.h"
#include "HashIndex.h"
#include "Meta.h"
#include "Record.h"
#include "RecordSchema.h"
#include "TableRecord.h"
#include "gallocator.h"
#include "tpcc/TpccConstants.h"
#include "tpcc/TpccKeyGenerator.h"
#include "tpcc/TpccParams.h"

// #include "Profiler.h"

namespace Database {
class Table : public GAMObject {
   public:
    Table() {
        schema_ptr_ = nullptr;
        primary_index_ = nullptr;
        secondary_indexes_ = nullptr;
    }
    ~Table() {
        if (primary_index_) {
            delete primary_index_;
            primary_index_ = nullptr;
        }
    }

    void Init(size_t table_id, RecordSchema* schema_ptr, GAlloc* gallocator) {
        table_id_ = table_id;
        schema_ptr_ = schema_ptr;
        secondary_count_ = 0;
        secondary_indexes_ = nullptr;

        primary_index_ = new HashIndex();
        primary_index_->Init(kHashIndexBucketHeaderNum, gallocator);
    }

    // return false if the key exists in primary index already
    bool InsertRecord(const IndexKey* keys, size_t key_num,
                      const GAddr& data_addr, GAlloc* gallocator,
                      size_t thread_id) {
        assert(key_num == secondary_count_ + 1);
        return primary_index_->InsertRecord(keys[0], data_addr, gallocator,
                                            thread_id);
    }

    GAddr SearchRecord(const IndexKey& key, GAlloc* gallocator,
                       size_t thread_id) {
        return primary_index_->SearchRecord(key, gallocator, thread_id);
    }

    // TODO(weihaosun): use record_size or table_record_size
    void ReportTableSize() const {
        uint64_t size =
            primary_index_->GetRecordCount() * schema_ptr_->GetSchemaSize();
        std::cout << "table_id=" << table_id_
                  << ", size=" << size * 1.0 / 1000 / 1000 << "MB" << std::endl;
    }

    size_t GetTableId() const { return table_id_; }
    size_t GetSecondaryCount() const { return secondary_count_; }
    RecordSchema* GetSchema() { return schema_ptr_; }
    size_t GetSchemaSize() const { return schema_ptr_->GetSchemaSize(); }
    HashIndex* GetPrimaryIndex() { return primary_index_; }

    virtual void Serialize(const GAddr& addr, GAlloc* gallocator) {
        size_t off = 0;
        gallocator->Write(addr, off, &table_id_, sizeof(size_t));
        off += sizeof(size_t);
        gallocator->Write(addr, off, &secondary_count_, sizeof(size_t));
        off += sizeof(size_t);
        GAddr cur_addr = GADD(addr, off);
        schema_ptr_->Serialize(cur_addr, gallocator);
        cur_addr = GADD(cur_addr, schema_ptr_->GetSerializeSize());
        primary_index_->Serialize(cur_addr, gallocator);
    }

    virtual void Deserialize(const GAddr& addr, GAlloc* gallocator) {
        size_t off = 0;
        gallocator->Read(addr, off, &table_id_, sizeof(size_t));
        off += sizeof(size_t);
        gallocator->Read(addr, off, &secondary_count_, sizeof(size_t));
        off += sizeof(size_t);
        GAddr cur_addr = GADD(addr, off);
        schema_ptr_ = new RecordSchema(table_id_);
        schema_ptr_->Deserialize(cur_addr, gallocator);
        cur_addr = GADD(cur_addr, schema_ptr_->GetSerializeSize());
        primary_index_ = new HashIndex();
        primary_index_->Deserialize(cur_addr, gallocator);
    }

    static size_t GetSerializeSize() {
        size_t ret = sizeof(size_t) * 2;
        ret += RecordSchema::GetSerializeSize();
        ret += HashIndex::GetSerializeSize();
        return ret;
    }

    void SaveCheckpoint(std::ofstream& out_stream, GAlloc* gallocator) {
        size_t record_size = schema_ptr_->GetSchemaSize();
        primary_index_->SaveCheckpoint(out_stream, record_size, gallocator,
                                       schema_ptr_);
    }
    void SaveCheckpointTpcc(std::ofstream& out_stream, GAlloc* gallocator,
                            map<Key, int>* OrderLineCnts) {
        size_t record_size = schema_ptr_->GetSchemaSize();
        primary_index_->SaveCheckpointTpcc(out_stream, record_size, gallocator,
                                           schema_ptr_, OrderLineCnts);
    }
    void ReloadCheckpointTpcc(std::ifstream& in_stream) {
        size_t record_size = schema_ptr_->GetSchemaSize();
        in_stream.seekg(0, std::ios::end);
        size_t file_size = static_cast<size_t>(in_stream.tellg());
        in_stream.seekg(0, std::ios::beg);
        size_t file_pos = 0;
        int item_w_id = TpccBenchmark::tpcc_scale_params.starting_warehouse_;
        while (file_pos < file_size) {
            char* entry = new char[record_size];
            in_stream.read(entry, record_size);
            Record* record = new Record(schema_ptr_, entry);
            if (schema_ptr_->GetTableId() ==
                TpccBenchmark::TableType::
                    ITEM_TABLE_ID) {  //目前这里只能处理dump的是分区的表的情况，如果dump的是全表，需要在item里增加wid项
                int iid = -1;
                record->GetColumn(0, &iid);

                if (item_w_id >
                    TpccBenchmark::tpcc_scale_params.ending_warehouse_)
                    item_w_id =
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_;

                TableRecord* table_record_buf = new TableRecord(record);

                Record* record_buf = table_record_buf->record_;
                GAlloc* gallocator = gallocators[0];
                GAddr data_addr = gallocator->AlignedMalloc(
                    table_record_buf->GetSerializeSize());
                record_buf->SetVisible(true);
                record_buf->Serialize(data_addr, gallocator);
                IndexKey key = TpccBenchmark::GetItemPrimaryKey(iid, item_w_id);
                InsertRecord(&key, 1, data_addr, gallocator, 0);
                delete table_record_buf;
                item_w_id++;
            } else if (schema_ptr_->GetTableId() ==
                       TpccBenchmark::TableType::WAREHOUSE_TABLE_ID) {
                int wid = -1;
                record->GetColumn(0, &wid);
                if (wid >=
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_ &&
                    wid <= TpccBenchmark::tpcc_scale_params.ending_warehouse_) {
                    TableRecord* table_record_buf = new TableRecord(record);
                    Record* record_buf = table_record_buf->record_;
                    GAlloc* gallocator = gallocators[0];
                    GAddr data_addr = gallocator->AlignedMalloc(
                        table_record_buf->GetSerializeSize());
                    record_buf->SetVisible(true);
                    record_buf->Serialize(data_addr, gallocator);
                    IndexKey key = TpccBenchmark::GetWarehousePrimaryKey(wid);
                    InsertRecord(&key, 1, data_addr, gallocator, 0);
                    delete table_record_buf;
                }
            } else if (schema_ptr_->GetTableId() ==
                       TpccBenchmark::TableType::DISTRICT_TABLE_ID) {
                int wid = -1;
                record->GetColumn(1, &wid);
                if (wid >=
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_ &&
                    wid <= TpccBenchmark::tpcc_scale_params.ending_warehouse_) {
                    TableRecord* table_record_buf = new TableRecord(record);
                    Record* record_buf = table_record_buf->record_;
                    GAlloc* gallocator = gallocators[0];
                    GAddr data_addr = gallocator->AlignedMalloc(
                        table_record_buf->GetSerializeSize());
                    record_buf->SetVisible(true);
                    record_buf->Serialize(data_addr, gallocator);
                    int did = -1;
                    record->GetColumn(0, &did);
                    IndexKey key =
                        TpccBenchmark::GetDistrictPrimaryKey(did, wid);
                    InsertRecord(&key, 1, data_addr, gallocator, 0);
                    delete table_record_buf;
                }
            } else if (schema_ptr_->GetTableId() ==
                       TpccBenchmark::TableType::CUSTOMER_TABLE_ID) {
                int wid = -1;
                record->GetColumn(2, &wid);
                if (wid >=
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_ &&
                    wid <= TpccBenchmark::tpcc_scale_params.ending_warehouse_) {
                    TableRecord* table_record_buf = new TableRecord(record);
                    Record* record_buf = table_record_buf->record_;
                    GAlloc* gallocator = gallocators[0];
                    GAddr data_addr = gallocator->AlignedMalloc(
                        table_record_buf->GetSerializeSize());
                    record_buf->SetVisible(true);
                    record_buf->Serialize(data_addr, gallocator);
                    int cid = -1;
                    record->GetColumn(0, &cid);
                    int cdid = -1;
                    record->GetColumn(1, &cdid);
                    IndexKey key =
                        TpccBenchmark::GetCustomerPrimaryKey(cid, cdid, wid);
                    InsertRecord(&key, 1, data_addr, gallocator, 0);
                    delete table_record_buf;
                }
            } else if (schema_ptr_->GetTableId() ==
                       TpccBenchmark::TableType::HISTORY_TABLE_ID) {
                int wid = -1;
                record->GetColumn(4, &wid);
                if (wid >=
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_ &&
                    wid <= TpccBenchmark::tpcc_scale_params.ending_warehouse_) {
                    TableRecord* table_record_buf = new TableRecord(record);
                    Record* record_buf = table_record_buf->record_;
                    GAlloc* gallocator = gallocators[0];
                    GAddr data_addr = gallocator->AlignedMalloc(
                        table_record_buf->GetSerializeSize());
                    record_buf->SetVisible(true);
                    record_buf->Serialize(data_addr, gallocator);
                    int hcid = -1;
                    record->GetColumn(0, &hcid);
                    int hdid = -1;
                    record->GetColumn(3, &hdid);
                    IndexKey key =
                        TpccBenchmark::GetHistoryPrimaryKey(hcid, hdid, wid);
                    InsertRecord(&key, 1, data_addr, gallocator, 0);
                    delete table_record_buf;
                }
            } else if (schema_ptr_->GetTableId() ==
                       TpccBenchmark::TableType::DISTRICT_NEW_ORDER_TABLE_ID) {
                int wid = -1;
                record->GetColumn(1, &wid);
                if (wid >=
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_ &&
                    wid <= TpccBenchmark::tpcc_scale_params.ending_warehouse_) {
                    TableRecord* table_record_buf = new TableRecord(record);
                    Record* record_buf = table_record_buf->record_;
                    GAlloc* gallocator = gallocators[0];
                    GAddr data_addr = gallocator->AlignedMalloc(
                        table_record_buf->GetSerializeSize());
                    record_buf->SetVisible(true);
                    record_buf->Serialize(data_addr, gallocator);
                    int did = -1;
                    record->GetColumn(0, &did);
                    IndexKey key =
                        TpccBenchmark::GetDistrictNewOrderPrimaryKey(did, wid);
                    InsertRecord(&key, 1, data_addr, gallocator, 0);
                    delete table_record_buf;
                }
            } else if (schema_ptr_->GetTableId() ==
                       TpccBenchmark::TableType::NEW_ORDER_TABLE_ID) {
                int wid = -1;
                record->GetColumn(2, &wid);
                if (wid >=
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_ &&
                    wid <= TpccBenchmark::tpcc_scale_params.ending_warehouse_) {
                    TableRecord* table_record_buf = new TableRecord(record);
                    Record* record_buf = table_record_buf->record_;
                    GAlloc* gallocator = gallocators[0];
                    GAddr data_addr = gallocator->AlignedMalloc(
                        table_record_buf->GetSerializeSize());
                    record_buf->SetVisible(true);
                    record_buf->Serialize(data_addr, gallocator);
                    int oid = -1;
                    record->GetColumn(0, &oid);
                    int did = -1;
                    record->GetColumn(1, &did);
                    IndexKey key =
                        TpccBenchmark::GetNewOrderPrimaryKey(oid, did, wid);
                    InsertRecord(&key, 1, data_addr, gallocator, 0);
                    delete table_record_buf;
                }
            } else if (schema_ptr_->GetTableId() ==
                       TpccBenchmark::TableType::ORDER_TABLE_ID) {
                int wid = -1;
                record->GetColumn(3, &wid);
                if (wid >=
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_ &&
                    wid <= TpccBenchmark::tpcc_scale_params.ending_warehouse_) {
                    TableRecord* table_record_buf = new TableRecord(record);
                    Record* record_buf = table_record_buf->record_;
                    GAlloc* gallocator = gallocators[0];
                    GAddr data_addr = gallocator->AlignedMalloc(
                        table_record_buf->GetSerializeSize());
                    record_buf->SetVisible(true);
                    record_buf->Serialize(data_addr, gallocator);
                    int oid = -1;
                    record->GetColumn(0, &oid);
                    int odid = -1;
                    record->GetColumn(2, &odid);
                    IndexKey key =
                        TpccBenchmark::GetOrderPrimaryKey(oid, odid, wid);
                    InsertRecord(&key, 1, data_addr, gallocator, 0);
                    delete table_record_buf;
                }
            } else if (schema_ptr_->GetTableId() ==
                       TpccBenchmark::TableType::ORDER_LINE_TABLE_ID) {
                int wid = -1;
                record->GetColumn(2, &wid);
                if (wid >=
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_ &&
                    wid <= TpccBenchmark::tpcc_scale_params.ending_warehouse_) {
                    TableRecord* table_record_buf = new TableRecord(record);
                    Record* record_buf = table_record_buf->record_;
                    GAlloc* gallocator = gallocators[0];
                    GAddr data_addr = gallocator->AlignedMalloc(
                        table_record_buf->GetSerializeSize());
                    record_buf->SetVisible(true);
                    record_buf->Serialize(data_addr, gallocator);
                    int oloid = -1;
                    record->GetColumn(0, &oloid);
                    int oldid = -1;
                    record->GetColumn(1, &oldid);
                    int olnumber = -1;
                    record->GetColumn(3, &olnumber);
                    IndexKey key = TpccBenchmark::GetOrderLinePrimaryKey(
                        oloid, oldid, wid, olnumber);
                    InsertRecord(&key, 1, data_addr, gallocator, 0);
                    delete table_record_buf;
                }
            } else if (schema_ptr_->GetTableId() ==
                       TpccBenchmark::TableType::STOCK_TABLE_ID) {
                int wid = -1;
                record->GetColumn(1, &wid);
                if (wid >=
                        TpccBenchmark::tpcc_scale_params.starting_warehouse_ &&
                    wid <= TpccBenchmark::tpcc_scale_params.ending_warehouse_) {
                    TableRecord* table_record_buf = new TableRecord(record);
                    Record* record_buf = table_record_buf->record_;
                    GAlloc* gallocator = gallocators[0];
                    GAddr data_addr = gallocator->AlignedMalloc(
                        table_record_buf->GetSerializeSize());
                    record_buf->SetVisible(true);
                    record_buf->Serialize(data_addr, gallocator);
                    int siid = -1;
                    record->GetColumn(0, &siid);
                    IndexKey key = TpccBenchmark::GetStockPrimaryKey(siid, wid);
                    InsertRecord(&key, 1, data_addr, gallocator, 0);
                    delete table_record_buf;
                }
            }
            // delete[] entry;
            // entry = nullptr;
            file_pos += record_size;
        }
    }

   private:
    size_t table_id_;
    size_t secondary_count_;

    RecordSchema* schema_ptr_;
    HashIndex* primary_index_;
    HashIndex** secondary_indexes_;  // Currently disabled
};                                   // namespace Database
}  // namespace Database
#endif
