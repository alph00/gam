// Copyright (c) 2018 The GAM Authors

#ifndef INCLUDE_DIRECTORY_H_
#define INCLUDE_DIRECTORY_H_

#include <list>
#include <unordered_map>
#include <set>

#include "hashtable.h"
#include "map.h"
#include "settings.h"
#include "structure.h"

enum DirState {
    DIR_UNSHARED,
    DIR_DIRTY,
    DIR_SHARED,
    DIR_TO_DIRTY,
    DIR_TO_SHARED,
    DIR_TO_UNSHARED
};

struct DirEntry {
    CacheState cache_state = CACHE_DIRTY;
    DirState state = DIR_UNSHARED;
    list<GAddr> shared;
    ptr_t addr;
    // if lock == 0, no one is holding the lock. otherwise, there are #lock ones
    // holding the lock but if lock = EXCLUSIVE_LOCK_TAG, it is a exclusive lock
    // int lock = 0;
#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
    unordered_map<ptr_t, pair<int, set<uint64_t>>> locks;
#else
    unordered_map<ptr_t, int> locks;
#endif
};

class Directory {
    /*
     * the directory structure
     * key: BLOCK-aligned address
     * value: directory entry
     */
    // unordered_map<ptr_t, DirEntry> dir;
#ifdef USE_SIMPLE_MAP
    Map<ptr_t, DirEntry*> dir{"directory"};
#else
    HashTable<ptr_t, DirEntry*> dir{"directory"};
#endif

    DirState GetState(ptr_t ptr);
    bool InTransitionState(ptr_t ptr);

#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
    inline bool CanWait(DirEntry* entry, ptr_t ptr, uint64_t timestamp) {
        epicAssert(entry->locks.count(ptr));
        epicAssert(entry->locks[ptr].second.find(timestamp) == entry->locks[ptr].second.end());
        uint64_t oldest_timestamp = *(entry->locks[ptr].second.begin());
        return oldest_timestamp > timestamp;
    }
    int RLock(ptr_t ptr, uint64_t timestamp);
    int RLock(DirEntry* entry, ptr_t ptr, uint64_t timestamp);
    bool IsRLocked(ptr_t ptr);
    bool IsRLocked(DirEntry* entry, ptr_t ptr);
    int WLock(ptr_t ptr, uint64_t timestamp);
    int WLock(DirEntry* entry, ptr_t ptr, uint64_t timestamp);
    bool IsWLocked(ptr_t ptr);
    bool IsWLocked(DirEntry* entry, ptr_t ptr);
    void UnLock(ptr_t ptr, uint64_t timestamp);
    void UnLock(DirEntry*& entry, ptr_t ptr, uint64_t timestamp);
    bool IsBlockWLocked(ptr_t block);
    bool IsBlockLocked(ptr_t block);

    int RLock(ptr_t ptr);
    int RLock(DirEntry* entry, ptr_t ptr);
    int WLock(ptr_t ptr);
    int WLock(DirEntry* entry, ptr_t ptr);
    void UnLock(ptr_t ptr);
    void UnLock(DirEntry*& entry, ptr_t ptr);
#else
    int RLock(ptr_t ptr);
    int RLock(DirEntry* entry, ptr_t ptr);
    bool IsRLocked(ptr_t ptr);
    bool IsRLocked(DirEntry* entry, ptr_t ptr);
    int WLock(ptr_t ptr);
    int WLock(DirEntry* entry, ptr_t ptr);
    bool IsWLocked(ptr_t ptr);
    bool IsWLocked(DirEntry* entry, ptr_t ptr);
    void UnLock(ptr_t ptr);
    void UnLock(DirEntry*& entry, ptr_t ptr);
    bool IsBlockWLocked(ptr_t block);
    bool IsBlockLocked(ptr_t block);
#endif

    DirEntry* GetEntry(ptr_t ptr) {
        if (dir.count(TOBLOCK(ptr))) {
            return dir.at(TOBLOCK(ptr));
        } else {
            return nullptr;
        }
    }

    void Clear(ptr_t ptr, GAddr addr);
    inline list<GAddr>& GetSList(ptr_t ptr) {
        return dir.at(TOBLOCK(ptr))->shared;
    }
    inline void lock(ptr_t ptr) {
        epicAssert(BLOCK_ALIGNED(ptr));
        LOCK_MICRO(dir, ptr);
    }
    inline void unlock(ptr_t ptr) {
        epicAssert(BLOCK_ALIGNED(ptr));
        UNLOCK_MICRO(dir, ptr);
    }

   public:
    /*
     * @ptr is the local virtual address
     */
    inline DirState GetState(void* ptr) { return GetState((ptr_t)ptr); }
    inline DirState GetState(DirEntry* entry) {
        if (!entry) {
            return DIR_UNSHARED;
        } else {
            return entry->state;
        }
    }
    inline bool InTransitionState(void* ptr) {
        return InTransitionState((ptr_t)ptr);
    }
    inline bool InTransitionState(DirState s) {
        return s == DIR_TO_DIRTY || s == DIR_TO_SHARED || s == DIR_TO_UNSHARED;
    }
    inline bool InTransitionState(DirEntry* entry) {
        if (!entry) {
            return false;
        } else {
            return InTransitionState(entry->state);
        }
    }
    DirEntry* GetEntry(void* ptr) { return GetEntry((ptr_t)ptr); }

    DirEntry* GetEntry(ptr_t ptr, GAddr addr) {
        ptr_t block = TOBLOCK(ptr);
        if (dir.count(block)) {
            return dir.at(block);
        } else {
            DirEntry* entry = new DirEntry();
            entry->state = DIR_UNSHARED;
            // entry->shared.push_back(addr);
            entry->addr = block;
            dir[block] = entry;
            return entry;
        }
    }

    DirEntry* InitEntry(ptr_t ptr, GAddr addr) {
        ptr_t block = TOBLOCK(ptr);
        if (dir.count(block)) {
            return dir.at(block);
        } else {
            DirEntry* entry = new DirEntry();
            entry->state = DIR_UNSHARED;
            // entry->shared.push_back(addr);
            entry->addr = block;
            dir[block] = entry;
            return entry;
        }
    }

    DirEntry* ToShared(void* ptr, GAddr addr);
    void ToShared(DirEntry* entry, GAddr addr);
    DirEntry* ToDirty(void* ptr, GAddr addr);
    void ToDirty(DirEntry* entry, GAddr addr);
    void ToUnShared(void* ptr);
    void ToUnShared(DirEntry*& entry);
    void ToToShared(void* ptr, GAddr addr = Gnullptr);
    void ToToShared(DirEntry* entry, GAddr addr = Gnullptr);
    DirEntry* ToToDirty(void* ptr, GAddr addr = Gnullptr);
    inline void ToToDirty(DirEntry* entry, GAddr = Gnullptr) {
        epicAssert(entry);
        entry->state = DIR_TO_DIRTY;
    }
    void ToToUnShared(void* ptr);
    void ToToUnShared(DirEntry* entry);
    void UndoDirty(void* ptr);
    void UndoDirty(DirEntry* entry);
    void UndoShared(void* ptr);
    void UndoShared(DirEntry* entry);
    void Remove(void* ptr, int wid);
    void Remove(DirEntry*& entry, int wid);

#ifdef ENABLE_LOCK_TIMESTAMP_CHECK

    inline bool CanWaitBlockLock(DirEntry* entry, uint64_t timestamp) {
        epicAssert(IsBlockLocked(entry));
        for (auto& lock_entry : entry->locks) {
            if (lock_entry.second.first > 0) {
                uint64_t cur_oldest_timestamp = *(lock_entry.second.second.begin());
                if (timestamp >= cur_oldest_timestamp) {
                    return false;
                }
            }
        }
        return true;
    }
    inline int RLock(void* ptr, uint64_t timestamp) { return RLock((ptr_t)ptr, timestamp); }
    inline int RLock(DirEntry* entry, void* ptr, uint64_t timestamp) {
        return RLock(entry, (ptr_t)ptr, timestamp);
    }
    inline int WLock(void* ptr, uint64_t timestamp) { return WLock((ptr_t)ptr, timestamp); }
    inline int WLock(DirEntry* entry, void* ptr, uint64_t timestamp) {
        return WLock(entry, (ptr_t)ptr, timestamp);
    }
    inline void UnLock(void* ptr, uint64_t timestamp) { UnLock((ptr_t)ptr, timestamp); }
    inline void UnLock(DirEntry*& entry, void* ptr, uint64_t timestamp) {
        UnLock(entry, (ptr_t)ptr, timestamp);
    }
#endif

    inline int RLock(void* ptr) { return RLock((ptr_t)ptr); }
    inline int RLock(DirEntry* entry, void* ptr) {
        return RLock(entry, (ptr_t)ptr);
    }
    int RLock(DirEntry* entry) = delete;

    inline bool IsRLocked(void* ptr) { return IsRLocked((ptr_t)ptr); }
    inline bool IsRLocked(DirEntry* entry, void* ptr) {
        return IsRLocked(entry, (ptr_t)ptr);
    }
    bool IsRLocked(DirEntry* entry) = delete;

    inline int WLock(void* ptr) { return WLock((ptr_t)ptr); }
    inline int WLock(DirEntry* entry, void* ptr) {
        return WLock(entry, (ptr_t)ptr);
    }
    int WLock(DirEntry* entry) = delete;

    inline bool IsWLocked(void* ptr) { return IsWLocked((ptr_t)ptr); }
    inline bool IsWLocked(DirEntry* entry, void* ptr) {
        return IsWLocked(entry, (ptr_t)ptr);
    }
    bool IsWLocked(DirEntry* entry) = delete;

    inline void UnLock(void* ptr) { UnLock((ptr_t)ptr); }
    inline void UnLock(DirEntry*& entry, void* ptr) {
        UnLock(entry, (ptr_t)ptr);
    }
    void UnLock(DirEntry* entry) = delete;

    inline bool IsBlockWLocked(void* block) {
        return IsBlockWLocked((ptr_t)block);
    }
    bool IsBlockWLocked(DirEntry* entry);
    inline bool IsBlockLocked(void* block) {
        return IsBlockLocked((ptr_t)block);
    }
    bool IsBlockLocked(DirEntry* entry);

    inline void Clear(void* ptr, GAddr addr) { Clear((ptr_t)ptr, addr); }
    void Clear(DirEntry*& entry, GAddr addr);

    inline list<GAddr>& GetSList(void* ptr) { return GetSList((ptr_t)ptr); }
    list<GAddr>& GetSList(DirEntry* entry) { return entry->shared; }

    // below are used for multithread programming
    inline void lock(void* ptr) { lock((ptr_t)ptr); }
    inline void unlock(void* ptr) { unlock((ptr_t)ptr); }
    void unlock(DirEntry* entry) = delete;
    void lock(DirEntry* entry) = delete;
};

#endif /* INCLUDE_DIRECTORY_H_ */
