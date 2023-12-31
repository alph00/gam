#ifndef __DATABASE_TXN_TIMESTAMP_MANAGER_H__
#define __DATABASE_TXN_TIMESTAMP_MANAGER_H__

#include "BatchTimestamp.h"
#include "Epoch.h"
#include "GlobalTimestamp.h"

namespace Database {
class TimestampManager {
   public:
    TimestampManager(GAddr epoch_addr, GAddr monotone_ts_addr, bool is_master,
                     GAlloc* epoch_gallocator)
        : epoch_(epoch_addr, is_master, epoch_gallocator),
          g_timestamp_(monotone_ts_addr),
          is_master_(is_master) {}
    ~TimestampManager() {
        // empty
    }

    uint64_t GetEpoch(GAlloc* gallocator) {
        return epoch_.GetEpoch(gallocator);
    }

    uint64_t GetMonotoneTimestamp(GAlloc* gallocator) {
        return g_timestamp_.GetMonotoneTimestamp(gallocator);
    }

    uint64_t GetBatchMonotoneTimestamp(GAlloc* gallocator) {
        return g_timestamp_.GetBatchMonotoneTimestamp(gallocator);
    }

    GAddr GetGlobalTsAddr() {
        return g_timestamp_.GetGlobalTsAddr();
    }

    bool NeedToInitLocalCacheTs() {
        return !is_master_;
    }

   private:
    Epoch epoch_;
    GlobalTimestamp g_timestamp_;
    bool is_master_;
};  // class TimestampManager
};  // namespace Database

#endif