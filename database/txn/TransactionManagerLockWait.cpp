// NOTICE: this file is adapted from Cavalia
#if defined(LOCK_WAIT)
#include "TransactionManager.h"

namespace Database {
bool TransactionManager::InsertRecord(TxnContext* context, size_t table_id,
                                      const IndexKey* keys, size_t key_num,
                                      Record* record, const GAddr& data_addr) {
    BEGIN_PHASE_MEASURE(thread_id_, CC_INSERT);

    if (is_first_access_ == true){
        BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
#if defined(BATCH_TIMESTAMP)
        if (!batch_ts_.IsAvailable()){
#ifdef USE_NOCACHE_TS
            uint64_t ts_holder = 0;
            gallocators[thread_id_]->GetAndAdvanceTs(GetGlobalTsAddr(), local_cache_ts_addr_, &ts_holder, kBatchTsNum);
            batch_ts_.InitTimestamp(ts_holder);
#else
            batch_ts_.InitTimestamp(GetMonotoneTimestamp());
#endif
        }
        start_timestamp_ = batch_ts_.GetTimestamp();
#else

#ifdef USE_NOCACHE_TS
        gallocators[thread_id_]->GetAndAdvanceTs(GetGlobalTsAddr(), local_cache_ts_addr_, &start_timestamp_, 1);
#else
        start_timestamp_ = GetMonotoneTimestamp();
#endif
#endif
        is_first_access_ = false;
        END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
    }

    // RecordSchema *schema_ptr =
    // storage_manager_->tables_[table_id]->GetSchema();
    TableRecord* table_record = new TableRecord(record);
    // TODO(weihaosun): no need to wlock now. Even if lock is needed later,
    // the lock should be put on the primary key rather than the thread local
    // record malloced BEGIN_GAM_OPERATION_MEASURE(thread_id_, GAM_TRY_WLOCK);
    // bool lock_success = TryWLockRecord(data_addr,
    // table_record->GetSerializeSize()); END_GAM_OPERATION_MEASURE(thread_id_,
    // GAM_TRY_WLOCK); assert(lock_success == true); // since record is thread
    // local
    record->SetVisible(true);
    Access* access = access_list_.NewAccess();
    access->access_type_ = INSERT_ONLY;
    access->access_record_ =
        table_record;  // should take ownership of table record(responsible for
                       // freeing memory)
    access->access_addr_ = data_addr;
    access->timestamp_ = 0;
    // PROFILE_TIME_START(thread_id_, INDEX_INSERT);
    // bool ret = storage_manager_->tables_[table_id]->InsertRecord(keys,
    // key_num, record->data_addr_, thread_id_);
    // PROFILE_TIME_END(thread_id_, INDEX_INSERT);
    END_PHASE_MEASURE(thread_id_, CC_INSERT);
    return true;
}

bool TransactionManager::SelectRecordCC(TxnContext* context, size_t table_id,
                                        Record*& record, const GAddr& data_addr,
                                        AccessType access_type) {
    epicLog(LOG_DEBUG,
            "thread_id=%u,table_id=%u,access_type=%u,data_addr=%lx, start "
            "SelectRecordCC",
            thread_id_, table_id, access_type, data_addr);
    BEGIN_PHASE_MEASURE(thread_id_, CC_SELECT);
    if (is_first_access_ == true){
        BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
#if defined(BATCH_TIMESTAMP)
        if (!batch_ts_.IsAvailable()){
#ifdef USE_NOCACHE_TS
            uint64_t ts_holder = 0;
            gallocators[thread_id_]->GetAndAdvanceTs(GetGlobalTsAddr(), local_cache_ts_addr_, &ts_holder, kBatchTsNum);
            batch_ts_.InitTimestamp(ts_holder);
#else
            batch_ts_.InitTimestamp(GetMonotoneTimestamp());
#endif
        }
        start_timestamp_ = batch_ts_.GetTimestamp();
#else

#ifdef USE_NOCACHE_TS
        gallocators[thread_id_]->GetAndAdvanceTs(GetGlobalTsAddr(), local_cache_ts_addr_, &start_timestamp_, 1);
#else
        start_timestamp_ = GetMonotoneTimestamp();
#endif
#endif
        is_first_access_ = false;
        END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
    }

    RecordSchema* schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
    record = new Record(schema_ptr);
    TableRecord* table_record = new TableRecord(record);
    bool lock_success = true;
    if (access_type == READ_ONLY) {
        BEGIN_GAM_OPERATION_MEASURE(thread_id_, GAM_TRY_RLOCK);
#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
        lock_success =
            TryRLockRecordWithTsCheck(data_addr, table_record->GetSerializeSize(), start_timestamp_);
#else
        lock_success =
            TryRLockRecord(data_addr, table_record->GetSerializeSize());
#endif
        END_GAM_OPERATION_MEASURE(thread_id_, GAM_TRY_RLOCK);
    } else {
        // DELETE_ONLY, READ_WRITE
        BEGIN_GAM_OPERATION_MEASURE(thread_id_, GAM_TRY_WLOCK);
#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
        lock_success =
            TryWLockRecordWithTsCheck(data_addr, table_record->GetSerializeSize(), start_timestamp_);
#else
        lock_success =
            TryWLockRecord(data_addr, table_record->GetSerializeSize());
#endif
        END_GAM_OPERATION_MEASURE(thread_id_, GAM_TRY_WLOCK);
    }

    if (lock_success) {
        // record = new Record(schema_ptr);
        // record->Deserialize(data_addr, gallocators[thread_id_]);
        BEGIN_GAM_OPERATION_MEASURE(thread_id_, GAM_READ);
        table_record->Deserialize(data_addr, gallocators[thread_id_]);
        END_GAM_OPERATION_MEASURE(thread_id_, GAM_READ);
        Access* access = access_list_.NewAccess();
        access->access_type_ = access_type;
        access->access_record_ = table_record;
        access->access_addr_ = data_addr;
        access->timestamp_ = 0;
        if (access_type == DELETE_ONLY) {
            record->SetVisible(false);
        }
        END_PHASE_MEASURE(thread_id_, CC_SELECT);
        return true;
    } else {  // fail to acquire lock
        delete table_record;
        END_PHASE_MEASURE(thread_id_, CC_SELECT);
        epicLog(LOG_DEBUG,
                "thread_id=%u,table_id=%u,access_type=%u,data_addr=%lx,lock "
                "fail, abort",
                thread_id_, table_id, access_type, data_addr);
        this->AbortTransaction();
        return false;
    }
}

bool TransactionManager::CommitTransaction(TxnContext* context, TxnParam* param,
                                           CharArray& ret_str) {
    epicLog(LOG_DEBUG, "thread_id=%u,txn_type=%d,commit", thread_id_,
            context->txn_type_);
    BEGIN_PHASE_MEASURE(thread_id_, CC_COMMIT);
    for (size_t i = 0; i < access_list_.access_count_; ++i) {
        Access* access = access_list_.GetAccess(i);
        assert(access->access_type_ == READ_ONLY ||
               access->access_type_ == DELETE_ONLY ||
               access->access_type_ == INSERT_ONLY ||
               access->access_type_ == READ_WRITE);
        // write back
        // Record *record = access->access_record_;
        TableRecord* table_record = access->access_record_;
        BEGIN_GAM_OPERATION_MEASURE(thread_id_, GAM_WRITE);
        if (access->access_type_ == READ_WRITE) {
            table_record->Serialize(access->access_addr_,
                                    gallocators[thread_id_]);
        } else if (access->access_type_ == DELETE_ONLY) {
            table_record->Serialize(access->access_addr_,
                                    gallocators[thread_id_]);
        }
        END_GAM_OPERATION_MEASURE(thread_id_, GAM_WRITE);
        // unlock
        if (access->access_addr_) {
            BEGIN_GAM_OPERATION_MEASURE(thread_id_, GAM_UNLOCK);
#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
            this->UnLockRecordWithTs(access->access_addr_,
                               table_record->GetSerializeSize(), start_timestamp_);
#else
            this->UnLockRecord(access->access_addr_,
                               table_record->GetSerializeSize());
#endif
            END_GAM_OPERATION_MEASURE(thread_id_, GAM_UNLOCK);
        }
    }
    // GC
    for (size_t i = 0; i < access_list_.access_count_; ++i) {
        Access* access = access_list_.GetAccess(i);
        if (access->access_type_ == DELETE_ONLY) {
            BEGIN_GAM_OPERATION_MEASURE(thread_id_, GAM_FREE);
            gallocators[thread_id_]->Free(access->access_addr_);
            END_GAM_OPERATION_MEASURE(thread_id_, GAM_FREE);
            access->access_addr_ = Gnullptr;
        }
        delete access->access_record_;
        access->access_record_ = nullptr;
        access->access_addr_ = Gnullptr;
    }
    access_list_.Clear();
    is_first_access_ = true;
    END_PHASE_MEASURE(thread_id_, CC_COMMIT);
    return true;
}

void TransactionManager::AbortTransaction() {
    epicLog(LOG_DEBUG, "thread_id=%u,abort", thread_id_);
    BEGIN_PHASE_MEASURE(thread_id_, CC_ABORT);
    for (size_t i = 0; i < access_list_.access_count_; ++i) {
        Access* access = access_list_.GetAccess(i);
        // Record *record = access->access_record_;
        TableRecord* table_record = access->access_record_;
        // unlock
        if (access->access_addr_) {
            BEGIN_GAM_OPERATION_MEASURE(thread_id_, GAM_UNLOCK);
#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
            this->UnLockRecordWithTs(access->access_addr_,
                               table_record->GetSerializeSize(), start_timestamp_);
#else
            this->UnLockRecord(access->access_addr_,
                               table_record->GetSerializeSize());
#endif
            END_GAM_OPERATION_MEASURE(thread_id_, GAM_UNLOCK);
        }
        // NOTE(weihaosun): currently we don't do GAM::MAlloc for INSERT_ONLY
        // records, thus no need to Free if (access->access_type_ ==
        // INSERT_ONLY) {
        //   table_record->record_->SetVisible(false);
        //   BEGIN_GAM_OPERATION_MEASURE(thread_id_, GAM_FREE);
        //   gallocators[thread_id_]->Free(access->access_addr_);
        //   END_GAM_OPERATION_MEASURE(thread_id_, GAM_FREE);
        // }
    }
    // GC
    for (size_t i = 0; i < access_list_.access_count_; ++i) {
        Access* access = access_list_.GetAccess(i);
        delete access->access_record_;
        access->access_record_ = nullptr;
        access->access_addr_ = Gnullptr;
    }
    access_list_.Clear();
    END_PHASE_MEASURE(thread_id_, CC_ABORT);
}
}  // namespace Database
#endif
 