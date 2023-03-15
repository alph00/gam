#ifndef __DATABASE_STORAGE_HASH_INDEX_H__
#define __DATABASE_STORAGE_HASH_INDEX_H__

#include <fstream>

#include "GAddrArray.h"
#include "HashIndexHelper.h"

// #include "Profiler.h"
#include "Profilers.h"
#include "Record.h"
#include "RecordSchema.h"
#include "gallocator.h"
#include "structure.h"
#include "tpcc/TpccConstants.h"
#include "tpcc/TpccKeyGenerator.h"
#include "tpcc/TpccParams.h"

namespace Database {
class HashIndex : public GAMObject {
   public:
    HashIndex() {}
    ~HashIndex() {}

    void Init(const uint64_t& buckets_count, GAlloc* gallocator) {
        buckets_count_ = buckets_count;
        record_count_ = 0;
        buckets_.Init(buckets_count_, BucketHeader::GetSerializeSize(),
                      gallocator);
        for (uint64_t i = 0; i < buckets_count_; ++i) {
            BucketHeader bk_hd;
            bk_hd.Init();
            GAddr bk_hd_addr = buckets_.GetElementGAddr(i, gallocator);
            bk_hd.Serialize(bk_hd_addr, gallocator);
        }
    }

    // return false if the key exists
    // Currently only one thread for populate and
    // InsertRecord is not called in execution
    // so InsertRecord is thread-safe
    bool InsertRecord(const IndexKey& key, const GAddr& record_ptr,
                      GAlloc* gallocator, size_t thread_id) {
        uint64_t offset = Hash(key);
        GAddr hd_addr = buckets_.GetElementGAddr(offset, gallocator);

        // PROFILE_TIME_START(thread_id, INDEX_INSERT_LOCK);
        BEGIN_PHASE_MEASURE(thread_id, INDEX_INSERT_LOCK);
        this->TryWLockBucketHeader(hd_addr, gallocator);
        // PROFILE_TIME_END(thread_id, INDEX_INSERT_LOCK);
        END_PHASE_MEASURE(thread_id, INDEX_INSERT_LOCK);

        // PROFILE_TIME_START(thread_id, INDEX_INSERT_GALLOCATE);
        BEGIN_PHASE_MEASURE(thread_id, INDEX_INSERT_GALLOCATE);
        BucketHeader bk_hd;
        bk_hd.Deserialize(hd_addr, gallocator);
        // PROFILE_TIME_END(thread_id, INDEX_INSERT_GALLOCATE);
        END_PHASE_MEASURE(thread_id, INDEX_INSERT_GALLOCATE);

        // PROFILE_TIME_START(thread_id, INDEX_INSERT_MUTATE);
        BEGIN_PHASE_MEASURE(thread_id, INDEX_INSERT_MUTATE);
        bool ret = bk_hd.InsertNode(key, record_ptr, gallocator);
        // PROFILE_TIME_END(thread_id, INDEX_INSERT_MUTATE);
        END_PHASE_MEASURE(thread_id, INDEX_INSERT_MUTATE);

        // PROFILE_TIME_START(thread_id, INDEX_INSERT_GALLOCATE);
        BEGIN_PHASE_MEASURE(thread_id, INDEX_INSERT_GALLOCATE);
        bk_hd.Serialize(hd_addr, gallocator);
        // PROFILE_TIME_END(thread_id, INDEX_INSERT_GALLOCATE);
        END_PHASE_MEASURE(thread_id, INDEX_INSERT_GALLOCATE);

        // PROFILE_TIME_START(thread_id, INDEX_INSERT_LOCK);
        BEGIN_PHASE_MEASURE(thread_id, INDEX_INSERT_LOCK);
        this->UnLock(hd_addr, gallocator);
        // PROFILE_TIME_END(thread_id, INDEX_INSERT_LOCK);
        END_PHASE_MEASURE(thread_id, INDEX_INSERT_LOCK);

        record_count_++;
        return ret;
    }

    // Currently no new record inserted in execution
    GAddr SearchRecord(const IndexKey& key, GAlloc* gallocator,
                       size_t thread_id) {
        uint64_t offset = Hash(key);
        GAddr hd_addr = buckets_.GetElementGAddr(offset, gallocator);
        // ensure that no inserts currently,
        // so disable lock for SearchRecord
        // this->TryRLockBucketHeader(hd_addr, gallocator);

        BucketHeader bk_hd;
        bk_hd.Deserialize(hd_addr, gallocator);
        GAddr items_list = Gnullptr;
        if (bk_hd.SearchNode(key, items_list, gallocator)) {
            Items items;
            gallocator->Read(items_list, &items, sizeof(Items));
            // this->UnLock(hd_addr, gallocator);
            //  Currently each record has different keys
            return items.record_ptrs_[0];
        } else {
            // this->UnLock(hd_addr, gallocator);
            return Gnullptr;
        }
    }

    virtual void Serialize(const GAddr& addr, GAlloc* gallocator) {
        size_t off = 0;
        buckets_.Serialize(addr, gallocator);
        off += buckets_.GetSerializeSize();
        gallocator->Write(addr, off, &buckets_count_, sizeof(uint64_t));
        off += sizeof(uint64_t);
        gallocator->Write(addr, off, &record_count_, sizeof(uint64_t));
    }
    virtual void Deserialize(const GAddr& addr, GAlloc* gallocator) {
        size_t off = 0;
        buckets_.Deserialize(addr, gallocator);
        off += buckets_.GetSerializeSize();
        gallocator->Read(addr, off, &buckets_count_, sizeof(uint64_t));
        off += sizeof(uint64_t);
        gallocator->Read(addr, off, &record_count_, sizeof(uint64_t));
    }
    static size_t GetSerializeSize() {
        return GAddrArray<BucketHeader>::GetSerializeSize() +
               sizeof(uint64_t) * 2;
    }
    uint64_t GetRecordCount() const { return record_count_; }
    uint64_t GetBucketsCount() const { return buckets_count_; }

    void SaveCheckpoint(std::ofstream& out_stream, const size_t& record_size,
                        GAlloc* gallocator, RecordSchema* schema) {
        Record* record = new Record(schema);
        for (auto i = 0; i < record_count_; ++i) {
            GAddr addr = SearchRecord((Key)i, gallocator, 0);
            record->Deserialize(addr, gallocator);
            out_stream.write(record->GetDataPtr(), record_size);
        }
        delete record;
        record = nullptr;
        out_stream.flush();
    }

    void SaveCheckpointTpcc(std::ofstream& out_stream,
                            const size_t& record_size, GAlloc* gallocator,
                            RecordSchema* schema,
                            map<Key, int>* OrderLineCnts) {
        Record* record = new Record(schema);
        //
        TpccBenchmark::TpccScaleParams* scale_params_ =
            &TpccBenchmark::tpcc_scale_params;
        IndexKey key;
        GAddr addr;

        if (schema->GetTableId() == TpccBenchmark::TableType::ITEM_TABLE_ID) {
            for (int item_id = 1; item_id <= scale_params_->num_items_;
                 ++item_id) {
                for (int w_id = scale_params_->starting_warehouse_;
                     w_id <= scale_params_->ending_warehouse_; ++w_id) {
                    key = TpccBenchmark::GetItemPrimaryKey(item_id, w_id);
                    addr = SearchRecord(key, gallocator, 0);
                    record->Deserialize(addr, gallocator);
                    out_stream.write(record->GetDataPtr(), record_size);

                    // int a0, a1;
                    // char a2[33];
                    // double a3;
                    // char a4[65];
                    // record->GetColumn(0, &a0);
                    // record->GetColumn(1, &a1);
                    // record->GetColumn(2, a2);
                    // a2[32] = '\0';
                    // record->GetColumn(3, &a3);
                    // record->GetColumn(4, a4);
                    // a4[64] = '\0';
                    // ofstream testout("table_data0_save2.txt", ios::app);
                    // testout << "~~" << a0 << " $ " << a1 << " $ " << a2 << "
                    // $ "
                    //         << a3 << " $ " << a4 << endl;
                    // testout.close();
                }
            }
        }
        // load warehouses
        for (int w_id = scale_params_->starting_warehouse_;
             w_id <= scale_params_->ending_warehouse_; ++w_id) {
            if (schema->GetTableId() ==
                TpccBenchmark::TableType::WAREHOUSE_TABLE_ID) {
                key = TpccBenchmark::GetWarehousePrimaryKey(w_id);
                addr = SearchRecord(key, gallocator, 0);
                record->Deserialize(addr, gallocator);
                out_stream.write(record->GetDataPtr(), record_size);
            }
            for (int d_id = 1;
                 d_id <= scale_params_->num_districts_per_warehouse_; ++d_id) {
                if (schema->GetTableId() ==
                    TpccBenchmark::TableType::DISTRICT_TABLE_ID) {
                    key = TpccBenchmark::GetDistrictPrimaryKey(d_id, w_id);
                    addr = SearchRecord(key, gallocator, 0);
                    record->Deserialize(addr, gallocator);
                    out_stream.write(record->GetDataPtr(), record_size);
                }
                for (int c_id = 1;
                     c_id <= scale_params_->num_customers_per_district_;
                     ++c_id) {
                    if (schema->GetTableId() ==
                        TpccBenchmark::TableType::CUSTOMER_TABLE_ID) {
                        key = TpccBenchmark::GetCustomerPrimaryKey(c_id, d_id,
                                                                   w_id);
                        addr = SearchRecord(key, gallocator, 0);
                        record->Deserialize(addr, gallocator);
                        out_stream.write(record->GetDataPtr(), record_size);
                    }
                    if (schema->GetTableId() ==
                        TpccBenchmark::TableType::HISTORY_TABLE_ID) {
                        key = TpccBenchmark::GetHistoryPrimaryKey(c_id, d_id,
                                                                  w_id);
                        addr = SearchRecord(key, gallocator, 0);
                        record->Deserialize(addr, gallocator);
                        out_stream.write(record->GetDataPtr(), record_size);
                    }
                }
                if (schema->GetTableId() ==
                    TpccBenchmark::TableType::DISTRICT_NEW_ORDER_TABLE_ID) {
                    key = TpccBenchmark::GetDistrictNewOrderPrimaryKey(d_id,
                                                                       w_id);
                    std::cout << ">>" << key << endl;
                    addr = SearchRecord(key, gallocator, 0);
                    record->Deserialize(addr, gallocator);
                    out_stream.write(record->GetDataPtr(), record_size);
                }

                int initial_new_order_id =
                    scale_params_->num_customers_per_district_ -
                    scale_params_->num_new_orders_per_district_ + 1;

                for (int o_id = 1;
                     o_id <= scale_params_->num_customers_per_district_;
                     ++o_id) {
                    if (schema->GetTableId() ==
                        TpccBenchmark::TableType::ORDER_TABLE_ID) {
                        key =
                            TpccBenchmark::GetOrderPrimaryKey(o_id, d_id, w_id);
                        addr = SearchRecord(key, gallocator, 0);
                        record->Deserialize(addr, gallocator);
                        out_stream.write(record->GetDataPtr(), record_size);
                        int cnt = -1;
                        record->GetColumn(6, &cnt);
                        (*OrderLineCnts)[key] = cnt;
                    }
                    if (schema->GetTableId() ==
                        TpccBenchmark::TableType::ORDER_LINE_TABLE_ID) {
                        IndexKey Orderkey =
                            TpccBenchmark::GetOrderPrimaryKey(o_id, d_id, w_id);
                        for (int ol_number = 1;
                             ol_number <= (*OrderLineCnts)[Orderkey];
                             ++ol_number) {
                            key = TpccBenchmark::GetOrderLinePrimaryKey(
                                o_id, d_id, w_id, ol_number);
                            addr = SearchRecord(key, gallocator, 0);
                            record->Deserialize(addr, gallocator);
                            out_stream.write(record->GetDataPtr(), record_size);
                        }
                    }
                    if (schema->GetTableId() ==
                        TpccBenchmark::TableType::NEW_ORDER_TABLE_ID) {
                        bool is_new_order = (o_id >= initial_new_order_id);
                        if (is_new_order) {
                            key = TpccBenchmark::GetNewOrderPrimaryKey(
                                o_id, d_id, w_id);
                            addr = SearchRecord(key, gallocator, 0);
                            record->Deserialize(addr, gallocator);
                            out_stream.write(record->GetDataPtr(), record_size);
                        }
                    }
                }
            }

            if (schema->GetTableId() ==
                TpccBenchmark::TableType::STOCK_TABLE_ID) {
                for (int i_id = 1; i_id <= scale_params_->num_items_; ++i_id) {
                    key = TpccBenchmark::GetStockPrimaryKey(i_id, w_id);
                    addr = SearchRecord(key, gallocator, 0);
                    record->Deserialize(addr, gallocator);
                    out_stream.write(record->GetDataPtr(), record_size);
                }
            }
        }
        //
        delete record;
        record = nullptr;
        out_stream.flush();
    }

   private:
    HashIndex(const HashIndex&);
    HashIndex& operator=(const HashIndex&);

    uint64_t Hash(const IndexKey& key) {
        uint64_t ret = key % buckets_count_;
        return ret;
    }
    /*ensure one thread at most hold one lock on bucketheader, no deadlock
     * concern */
    void TryWLockBucketHeader(const GAddr& header_addr, GAlloc* gallocator) {
        size_t sz = BucketHeader::GetSerializeSize();
        gallocator->WLock(header_addr, sz);
    }
    void TryRLockBucketHeader(const GAddr& header_addr, GAlloc* gallocator) {
        size_t sz = BucketHeader::GetSerializeSize();
        gallocator->RLock(header_addr, sz);
    }
    void UnLock(const GAddr& header_addr, GAlloc* gallocator) {
        size_t sz = BucketHeader::GetSerializeSize();
        gallocator->UnLock(header_addr, sz);
    }

   private:
    GAddrArray<BucketHeader> buckets_;
    uint64_t buckets_count_;
    uint64_t record_count_;
};
}  // namespace Database

#endif
