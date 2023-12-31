// Copyright (c) 2018 The GAM Authors

int Worker::ProcessLocalRead(WorkRequest* wr) {
    epicAssert(wr->addr);
    epicAssert(!(wr->flag & ASYNC));

    if (!(wr->flag & FENCE)) {
        Fence* fence = fences_.at(wr->fd);
        fence->lock();
        if (unlikely(IsMFenced(fence, wr))) {
            AddToFence(fence, wr);
            epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
                    fence->mfenced, fence->sfenced, wr->op);
            fence->unlock();
            return FENCE_PENDING;
        }
        fence->unlock();
    }

    if (likely(IsLocal(wr->addr))) {
        GAddr start = wr->addr;
        GAddr start_blk = TOBLOCK(start);
        GAddr end = GADD(start, wr->size);
        wr->lock();
        /*
         * we increase it by 1 before we push to the to_serve_local_request
         * queue so we have to decrease by 1 again
         */
        if (wr->flag & TO_SERVE) {
            wr->counter--;
        }
        for (GAddr i = start_blk; i < end;) {
            GAddr nextb = BADD(i, 1);
            void* laddr = ToLocal(i);

            directory.lock(laddr);
            DirEntry* entry = directory.GetEntry(laddr);
            DirState s = directory.GetState(entry);
            if (unlikely(directory.InTransitionState(s))) {
                epicLog(LOG_INFO,
                        "directory in transition state when local read %d", s);
                // we increase the counter in case
                // we false call Notify()
                wr->counter++;
                AddToServeLocalRequest(i, wr);
                directory.unlock(laddr);
                wr->unlock();
                wr->is_cache_hit_ = false;
                return IN_TRANSITION;
            }

            if (unlikely(s == DIR_DIRTY)) {
                WorkRequest* lwr = new WorkRequest(*wr);
                lwr->counter = 0;
                GAddr rc =
                    directory.GetSList(entry)
                        .front();  // only one worker is updating this line
                Client* cli = GetClient(rc);
                lwr->op = FETCH_AND_SHARED;
                lwr->addr = i;
                lwr->size = BLOCK_SIZE;
                lwr->ptr = laddr;
                lwr->parent = wr;
#ifndef NDEBUG
                wr->child = lwr;
#endif
                wr->counter++;
                wr->is_cache_hit_ = false;
                // intermediate state
                epicAssert(s != DIR_TO_SHARED);
                epicAssert(!directory.IsBlockLocked(entry));
                directory.ToToShared(entry, rc);
                SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
            } else {
                GAddr gs = i > start ? i : start;
                void* ls = (void*)((ptr_t)wr->ptr + GMINUS(gs, start));
                int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
                memcpy(ls, ToLocal(gs), len);
            }
            directory.unlock(laddr);
            i = nextb;
        }
        if (unlikely(wr->counter)) {
            wr->unlock();
            return REMOTE_REQUEST;
        } else {
            wr->unlock();
        }
    } else {
        int ret = cache.Read(wr);
        if (ret) return REMOTE_REQUEST;
    }

#ifdef MULTITHREAD
    if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
        /*
         * notify the app thread directly
         * this can only happen when the request can be fulfilled locally
         * or we don't need to wait for reply from remote node
         */
        if (Notify(wr)) {
            epicLog(LOG_WARNING, "cannot wake up the app thread");
        }
#ifdef MULTITHREAD
    } else {
        /**
         * In this case, the read request is running in the app thread and
         * is fulfilled in the first trial (i.e., * chache hit)
         */
        epicAssert(wr->is_cache_hit_);
#ifdef ENABLE_STATISTICS
        if (IsLocal(wr->addr)) {
            no_local_reads_.fetch_add(1, std::memory_order_relaxed);
            no_local_reads_hit_.fetch_add(1, std::memory_order_relaxed);
        } else {
            no_remote_reads_.fetch_add(1, std::memory_order_relaxed);
            no_remote_reads_hit_.fetch_add(1, std::memory_order_relaxed);
        }
#endif
    }
#endif
    return SUCCESS;
}

int Worker::ProcessLocalWrite(WorkRequest* wr) {
    epicAssert(wr->addr);
    Fence* fence = fences_.at(wr->fd);
    if (!(wr->flag & FENCE)) {
        fence->lock();
        if (unlikely(IsFenced(fence, wr))) {
            epicLog(LOG_DEBUG, "fenced(mfenced = %d, sfenced = %d): %d",
                    fence->mfenced, fence->sfenced, wr->op);
            AddToFence(fence, wr);
            fence->unlock();
            return FENCE_PENDING;
        }
        fence->unlock();
    }
    if ((wr->flag & ASYNC) && !(wr->flag & TO_SERVE)) {
        fence->pending_writes++;
        epicLog(LOG_DEBUG, "Local: one more pending write");
    }
    if (likely(IsLocal(wr->addr))) {
        GAddr start = wr->addr;
        GAddr start_blk = TOBLOCK(start);
        GAddr end = GADD(start, wr->size);
        if (TOBLOCK(end - 1) != start_blk) {
            epicLog(LOG_INFO, "read/write split to multiple blocks");
        }
        wr->lock();
        /*
         * we increase it by 1 before we push to the to_serve_local_request
         * queue so we have to decrease by 1 again
         */
        if (wr->flag & TO_SERVE) {
            wr->counter--;
        }
        for (GAddr i = start_blk; i < end;) {
            epicAssert(!(wr->flag & COPY) ||
                       ((wr->flag & COPY) && (wr->flag & ASYNC)));

            GAddr nextb = BADD(i, 1);
            void* laddr = ToLocal(i);

            directory.lock(laddr);
            DirEntry* entry = directory.GetEntry(laddr);
            DirState state = directory.GetState(entry);
            if (unlikely(directory.InTransitionState(state))) {
                epicLog(LOG_INFO,
                        "directory in transition state when local write %d",
                        state);
                // we increase the counter in case
                // we false call Notify()
                wr->counter++;
                wr->is_cache_hit_ = false;
                if (wr->flag & ASYNC) {
                    if (!wr->IsACopy()) {
                        wr->unlock();
                        wr = wr->Copy();
                        wr->lock();
                    }
                }
                AddToServeLocalRequest(i, wr);
                directory.unlock(laddr);
                wr->unlock();
                return IN_TRANSITION;
            }

            /*
             * since we cannot guarantee that generating a completion indicates
             * the buf in the remote node has been updated (only means remote
             * HCA received and acked) (ref:
             * http://lists.openfabrics.org/pipermail/general/2007-May/036615.html)
             * so we use Request/Reply mode even for DIR_SHARED invalidations
             * instead of direct WRITE or CAS to invalidate the corresponding
             * cache line in remote node
             */
            if (state == DIR_DIRTY || state == DIR_SHARED) {
                list<GAddr>& shared = directory.GetSList(entry);
                WorkRequest* lwr = new WorkRequest(*wr);
                lwr->counter = 0;
                lwr->op =
                    state == DIR_DIRTY ? FETCH_AND_INVALIDATE : INVALIDATE;
                lwr->addr = i;
                lwr->size = BLOCK_SIZE;
                lwr->ptr = laddr;
                wr->is_cache_hit_ = false;
                if (wr->flag & ASYNC) {
                    if (!wr->IsACopy()) {
                        wr->unlock();
                        wr = wr->Copy();
                        wr->lock();
                    }
                }
                lwr->parent = wr;
#ifndef NDEBUG
                wr->child = lwr;
#endif
                lwr->id = GetWorkPsn();
                lwr->counter = shared.size();
                wr->counter++;
                epicAssert(state != DIR_TO_UNSHARED);
                epicAssert(
                    (state == DIR_DIRTY && !directory.IsBlockLocked(entry)) ||
                    (state == DIR_SHARED && !directory.IsBlockWLocked(entry)));
                directory.ToToUnShared(entry);
                // we move AddToPending before submit request
                // since it is possible that the reply comes back before we add
                // to the pending list if we AddToPending at last
                AddToPending(lwr->id, lwr);
                for (auto it = shared.begin(); it != shared.end(); it++) {
                    Client* cli = GetClient(*it);
                    epicLog(LOG_DEBUG,
                            "invalidate (%d) cache from worker %d (lwr = %lx)",
                            lwr->op, cli->GetWorkerId(), lwr);
                    SubmitRequest(cli, lwr);
                    // lwr->counter++;
                }
            } else {
#ifdef GFUNC_SUPPORT
                if (wr->flag & GFUNC) {
                    epicAssert(wr->gfunc);
                    epicAssert(TOBLOCK(wr->addr) ==
                               TOBLOCK(GADD(wr->addr, wr->size - 1)));
                    epicAssert(i == start_blk);
                    void* laddr = ToLocal(wr->addr);
                    wr->gfunc(laddr, wr->arg);
                } else {
#endif
                    GAddr gs = i > start ? i : start;
                    void* ls = (void*)((ptr_t)wr->ptr + GMINUS(gs, start));
                    int len = nextb > end ? GMINUS(end, gs) : GMINUS(nextb, gs);
                    memcpy(ToLocal(gs), ls, len);
                    epicLog(LOG_DEBUG, "copy dirty data in advance");
#ifdef GFUNC_SUPPORT
                }
#endif
            }
            directory.unlock(laddr);
            i = nextb;
        }
        if (wr->counter) {
            wr->unlock();
            return REMOTE_REQUEST;
        } else {
            wr->unlock();
        }
    } else {
        int ret = cache.Write(wr);
        if (ret) {
            return REMOTE_REQUEST;
        }
#ifdef ENABLE_STATISTICS
        no_remote_writes_direct_hit_.fetch_add(1, std::memory_order_relaxed);
#endif
    }
#ifdef MULTITHREAD
    if (wr->flag & ASYNC || wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
        /*
         * notify the app thread directly
         * this can only happen when the request can be fulfilled locally
         * or we don't need to wait for reply from remote node
         */
        if (Notify(wr)) {
            epicLog(LOG_WARNING, "cannot wake up the app thread");
        }
#ifdef MULTITHREAD
    }
#endif
    return SUCCESS;
}

int Worker::ProcessLocalRLock(WorkRequest* wr) {
    epicAssert(wr->addr);
    epicAssert(!(wr->flag & ASYNC));
    // epicAssert(!(wr->flag & FENCE));
    if (!(wr->flag & FENCE)) {
        Fence* fence = fences_.at(wr->fd);
        fence->lock();
        if (IsFenced(fence, wr)) {
            AddToFence(fence, wr);
            fence->unlock();
            epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
                    fence->mfenced, fence->sfenced, wr->op);
            return FENCE_PENDING;
        } else if (fence->pending_writes) {  // we only mark fenced when there
                                             // are pending writes
            fence->mfenced = true;
            epicLog(LOG_DEBUG, "mfenced from RLOCK!");
            AddToFence(fence, wr);
            fence->unlock();
            return FENCE_PENDING;
        }
        fence->unlock();
    }
    if (IsLocal(wr->addr)) {
        GAddr start = wr->addr;
        GAddr start_blk = TOBLOCK(start);
        void* laddr = ToLocal(start_blk);

        wr->lock();
        directory.lock(laddr);
        DirEntry* entry = directory.GetEntry(laddr);
        DirState state = directory.GetState(entry);
        if (directory.InTransitionState(state)) {
            epicLog(LOG_INFO,
                    "directory in transition state when local read %d", state);
            AddToServeLocalRequest(start_blk, wr);
            directory.unlock(laddr);
            wr->unlock();
            return IN_TRANSITION;
        }
        if (state == DIR_DIRTY) {
            WorkRequest* lwr = new WorkRequest(*wr);
            lwr->counter = 0;
            GAddr rc = directory.GetSList(entry)
                           .front();  // only one worker is updating this line
            Client* cli = GetClient(rc);
            lwr->op = FETCH_AND_SHARED;
            lwr->addr = start_blk;
            lwr->size = BLOCK_SIZE;
            lwr->ptr = laddr;
            lwr->parent = wr;
#ifndef NDEBUG
            wr->child = lwr;
#endif
            lwr->flag |= LOCKED;
            wr->counter++;
            // intermediate state
            epicAssert(state != DIR_TO_SHARED);
            epicAssert(!directory.IsBlockLocked(entry));
            directory.ToToShared(entry, rc);
            SubmitRequest(cli, lwr, ADD_TO_PENDING | REQUEST_SEND);
        } else {
            int ret;
#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
            if (wr->flag & WITH_TS_CHECK) {
                if (entry) {
                    ret = directory.RLock(entry, ToLocal(wr->addr), wr->lock_ts);
                } else {
                    ret = directory.RLock(ToLocal(wr->addr), wr->lock_ts);
                }
                if (ret < 0) {  // fail to lock now and can not wait
                    epicLog(LOG_INFO, "cannot lock addr %lx, will try later",
                            wr->addr);
                    wr->status = LOCK_FAILED;
                } else if (ret > 0) {  // fail to lock now but can wait
                    epicLog(LOG_INFO, "cannot lock addr %lx, will wait",
                            wr->addr);
                    AddToServeLocalRequest(start_blk, wr);
                    directory.unlock(laddr);
                    wr->unlock();
                    return IN_TRANSITION;
                }
            } else {
                if (entry) {
                    ret = directory.RLock(entry, ToLocal(wr->addr));
                } else {
                    ret = directory.RLock(ToLocal(wr->addr));
                }
                if (ret) {  // fail to lock
                    epicLog(LOG_INFO, "cannot lock addr %lx, will try later",
                            wr->addr);
                    if (wr->flag & TRY_LOCK) {
                        wr->status = LOCK_FAILED;
                    } else {
                        AddToServeLocalRequest(start_blk, wr);
                        directory.unlock(laddr);
                        wr->unlock();
                        return IN_TRANSITION;
                    }
                }
            }
#else
            if (entry) {
                ret = directory.RLock(entry, ToLocal(wr->addr));
            } else {
                ret = directory.RLock(ToLocal(wr->addr));
            }
            if (ret) {  // fail to lock
                epicLog(LOG_INFO, "cannot lock addr %lx, will try later",
                        wr->addr);
                if (wr->flag & TRY_LOCK) {
                    wr->status = LOCK_FAILED;
                } else {
                    AddToServeLocalRequest(start_blk, wr);
                    directory.unlock(laddr);
                    wr->unlock();
                    return IN_TRANSITION;
                }
            }
#endif
        }
        if (wr->counter) {
            directory.unlock(laddr);
            wr->unlock();
            return REMOTE_REQUEST;
        } else {
            directory.unlock(laddr);
            wr->unlock();
        }
    } else {
        // if there are remote requests
        int ret = cache.RLock(wr);
        if (ret) {
            return REMOTE_REQUEST;
        }
    }
#ifdef MULTITHREAD
    if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
        /*
         * notify the app thread directly
         * this can only happen when the request can be fulfilled locally
         * or we don't need to wait for reply from remote node
         */
        if (Notify(wr)) {
            epicLog(LOG_WARNING, "cannot wake up the app thread");
        }
#ifdef MULTITHREAD
    } else {
        epicAssert(wr->is_cache_hit_ || (wr->flag & TRY_LOCK));
#ifdef ENABLE_STATISTICS
        if (IsLocal(wr->addr)) {
            no_local_reads_.fetch_add(1, std::memory_order_relaxed);
            ;
            no_local_reads_hit_.fetch_add(1, std::memory_order_relaxed);
            ;
        } else {
            no_remote_reads_.fetch_add(1, std::memory_order_relaxed);
            ;
            no_remote_reads_hit_.fetch_add(1, std::memory_order_relaxed);
            ;
        }
#endif
    }
#endif
    return SUCCESS;
}

int Worker::ProcessLocalWLock(WorkRequest* wr) {
    epicAssert(wr->addr);
    epicAssert(!(wr->flag & ASYNC));
    if (!(wr->flag & FENCE)) {
        Fence* fence = fences_.at(wr->fd);
        fence->lock();
        if (IsFenced(fence, wr)) {
            AddToFence(fence, wr);
            fence->unlock();
            epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
                    fence->mfenced, fence->sfenced, wr->op);
            return FENCE_PENDING;
        } else if (fence->pending_writes) {  // we only mark fenced when there
                                             // are pending writes
            fence->mfenced = true;
            epicLog(LOG_DEBUG, "mfenced from WLOCK!");
            AddToFence(fence, wr);
            fence->unlock();
            return FENCE_PENDING;
        }
        fence->unlock();
    }
    if (IsLocal(wr->addr)) {
        GAddr start = wr->addr;
        GAddr start_blk = TOBLOCK(start);
        void* laddr = ToLocal(start_blk);

        wr->lock();
        directory.lock(laddr);
        DirEntry* entry = directory.GetEntry(laddr);
        DirState state = directory.GetState(entry);
        if (directory.InTransitionState(state)) {
            epicLog(LOG_INFO,
                    "directory in transition state when local write %d", state);
            AddToServeLocalRequest(start_blk, wr);
            directory.unlock(laddr);
            wr->unlock();
            return IN_TRANSITION;
        }
        if (DIR_DIRTY == state || DIR_SHARED == state) {
            list<GAddr>& shared = directory.GetSList(entry);
            WorkRequest* lwr = new WorkRequest(*wr);
            lwr->counter = 0;
            lwr->op = state == DIR_DIRTY ? FETCH_AND_INVALIDATE : INVALIDATE;
            lwr->addr = start_blk;
            lwr->size = BLOCK_SIZE;
            lwr->ptr = laddr;
            lwr->parent = wr;
#ifndef NDEBUG
            wr->child = lwr;
#endif
            lwr->flag |= LOCKED;
            lwr->id = GetWorkPsn();
            lwr->counter = shared.size();
            wr->counter++;
            epicAssert(state != DIR_TO_UNSHARED);
            epicAssert(
                (state == DIR_DIRTY && !directory.IsBlockLocked(entry)) ||
                (state == DIR_SHARED && !directory.IsBlockWLocked(entry)));
            directory.ToToUnShared(entry);
            AddToPending(lwr->id, lwr);
            for (auto it = shared.begin(); it != shared.end(); it++) {
                Client* cli = GetClient(*it);
                epicLog(LOG_DEBUG,
                        "invalidate (%d) cache from worker %d, state = %d, "
                        "lwr->counter = %d",
                        lwr->op, cli->GetWorkerId(), state,
                        lwr->counter.load());
                SubmitRequest(cli, lwr);
                // lwr->counter++;
            }
        } else if (DIR_UNSHARED == state) {
            int ret;
#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
            if (wr->flag & WITH_TS_CHECK) {
                epicAssert(wr->flag & TRY_LOCK);
                if (entry) {
                    ret = directory.WLock(entry, ToLocal(wr->addr), wr->lock_ts);
                } else {
                    ret = directory.WLock(ToLocal(wr->addr), wr->lock_ts);
                }
                if (ret < 0) {  // failed to lock now and can not wait
                    epicLog(LOG_INFO, "cannot lock addr %lx, will try later",
                            wr->addr);
                    wr->status = LOCK_FAILED;
                } else if (ret > 0) {  // failed to lock now but can wait
                    epicLog(LOG_INFO, "cannot lock addr %lx, will wait",
                            wr->addr);
                    AddToServeLocalRequest(start_blk, wr);
                    directory.unlock(laddr);
                    wr->unlock();
                    return IN_TRANSITION;
                }
            } else {
                if (entry) {
                    ret = directory.WLock(entry, ToLocal(wr->addr));
                } else {
                    ret = directory.WLock(ToLocal(wr->addr));
                }
                if (ret) {  // failed to lock
                    epicLog(LOG_INFO, "cannot lock addr %lx, will try later",
                            wr->addr);
                    if (wr->flag & TRY_LOCK) {
                        wr->status = LOCK_FAILED;
                    } else {
                        AddToServeLocalRequest(start_blk, wr);
                        directory.unlock(laddr);
                        wr->unlock();
                        return IN_TRANSITION;
                    }
                }
            }
#else
            if (entry) {
                ret = directory.WLock(entry, ToLocal(wr->addr));
            } else {
                ret = directory.WLock(ToLocal(wr->addr));
            }
            if (ret) {  // failed to lock
                epicLog(LOG_INFO, "cannot lock addr %lx, will try later",
                        wr->addr);
                if (wr->flag & TRY_LOCK) {
                    wr->status = LOCK_FAILED;
                } else {
                    AddToServeLocalRequest(start_blk, wr);
                    directory.unlock(laddr);
                    wr->unlock();
                    return IN_TRANSITION;
                }
            }
#endif
        }
        if (wr->counter) {
            directory.unlock(laddr);
            wr->unlock();
            return REMOTE_REQUEST;
        } else {
            directory.unlock(laddr);
            wr->unlock();
        }
    } else {
        int ret = cache.WLock(wr);
        if (ret) {
            return REMOTE_REQUEST;
        }
    }
#ifdef MULTITHREAD
    if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
        /*
         * notify the app thread directly
         * this can only happen when the request can be fulfilled locally
         * or we don't need to wait for reply from remote node
         */
        if (Notify(wr)) {
            epicLog(LOG_WARNING, "cannot wake up the app thread");
        }
#ifdef MULTITHREAD
    } else {
        epicAssert(wr->is_cache_hit_ || (wr->flag & TRY_LOCK));

#ifdef ENABLE_STATISTICS
        if (IsLocal(wr->addr)) {
            no_local_writes_.fetch_add(1, std::memory_order_relaxed);
            ;
            no_local_writes_hit_.fetch_add(1, std::memory_order_relaxed);
            ;
        } else {
            no_remote_writes_.fetch_add(1, std::memory_order_relaxed);
            ;
            no_remote_writes_hit_.fetch_add(1, std::memory_order_relaxed);
            ;
        }
#endif
    }

#endif
    return SUCCESS;
}

int Worker::ProcessLocalUnLock(WorkRequest* wr) {
    if (!(wr->flag & FENCE)) {
        Fence* fence = fences_.at(wr->fd);
        fence->lock();
        if (IsFenced(fence, wr)) {
            AddToFence(fence, wr);
            fence->unlock();
            epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
                    fence->mfenced, fence->sfenced, wr->op);
            return FENCE_PENDING;
        } else if (fence->pending_writes) {  // we only mark fenced when there
                                             // are pending writes
            fence->mfenced = true;
            epicLog(LOG_DEBUG, "mfenced from UNLOCK!");
            AddToFence(fence, wr);
            fence->unlock();
            return FENCE_PENDING;
        }
        fence->unlock();
    }

    if (IsLocal(wr->addr)) {
        GAddr start_blk = TOBLOCK(wr->addr);
        void* laddr = ToLocal(start_blk);
        directory.lock(laddr);
#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
        if (wr->flag & WITH_TS_CHECK) {
            directory.UnLock(ToLocal(wr->addr), wr->lock_ts);
        } else {
            directory.UnLock(ToLocal(wr->addr));
        }
#else
        directory.UnLock(ToLocal(wr->addr));
#endif
        directory.unlock(laddr);
    } else {
#ifdef ENABLE_LOCK_TIMESTAMP_CHECK
        if (wr->flag & WITH_TS_CHECK) {
            cache.UnLock(wr->addr, wr->lock_ts);
        } else {
            cache.UnLock(wr->addr);
        }
#else
        cache.UnLock(wr->addr);
#endif
    }
    ProcessToServeRequest(wr);
#ifdef MULTITHREAD
    if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
        /*
         * notify the app thread directly
         * this can only happen when the request can be fulfilled locally
         * or we don't need to wait for reply from remote node
         */
        if (Notify(wr)) {
            epicLog(LOG_WARNING, "cannot wake up the app thread");
        }
#ifdef MULTITHREAD
    }
#endif
    return SUCCESS;
}

// bool Worker::ProcessLocalVersionCheck(const GAddr base_addr, const Size count, const Size offset, uint64_t version) {
//     GAddr start = base_addr;
//     GAddr start_blk = TOBLOCK(start);
//     GAddr end = GADD(start, count);
//     if (TOBLOCK(end - 1) != start_blk) {
//         epicLog(LOG_INFO, "read/write split to multiple blocks");
//     }
//     GAddr check_start = base_addr + offset;
//     GAddr check_end = check_start + sizeof(version);
//     uint64_t actual_version;
//     // char* version_bytes = (char*)(&version);
//     // int bytes_to_check = sizeof(version);
//     if (IsLocal(base_addr)) {
//         // now assume the data doesn't cross nodes
//         epicAssert(IsLocal(end));
//         for (GAddr i = start_blk; i < end;) {
//             GAddr nextb = BADD(i, 1);
//             void* laddr = ToLocal(i);
//             directory.lock(laddr);
//             DirEntry* entry = directory.GetEntry(laddr);
//             // check state and lock
//             if (entry && (directory.InTransitionState(entry) || entry->state == DIR_DIRTY || directory.IsWLocked(entry, laddr))) {
//                 return false;
//             }

//             if (likely(check_start < nextb && check_end > i)) {
//                 GAddr gs = i > check_start ? i : check_start;
//                 void* dest = (void*)((ptr_t)(&actual_version) + GMINUS(gs, check_start));
//                 int len = nextb > check_end ? GMINUS(check_end, gs) : GMINUS(nextb, gs);
//                 memcpy(dest, ToLocal(gs), len);
//             }

//             directory.unlock(laddr);
//             i = nextb;
//         }
//     } else {

//         for (GAddr i = start_blk; i < end;) {
//             GAddr nextb = BADD(i, 1);
//             cache.lock(i);
//             CacheLine* cline = cache.GetCLine(i);
//             // check state and lock
//             if (!cline || cache.InTransitionState(cline) || cache.IsWLocked(i)) {
//                 return false;
//             }

//             epicAssert(cline->state == CACHE_SHARED || cline->state == CACHE_DIRTY);

//             if (likely(check_start < nextb && check_end > i)) {
//                 GAddr gs = i > check_start ? i : check_start;
//                 void* source = (void*)((ptr_t)(cline->line) + GMINUS(gs, i));
//                 void* dest = (void*)((ptr_t)(&actual_version) + GMINUS(gs, check_start));
//                 int len = nextb > check_end ? GMINUS(check_end, gs) : GMINUS(nextb, gs);
//                 memcpy(dest, source, len);
//             }



//             // if (bytes_to_check > 0 && likely(check_start < nextb)) {
//             //     char* line_bytes = (char*)(cline->line) + (check_start - i);
//             //     if (likely(check_end <= nextb)) {
//             //         // all bytes_to_check in one block
//             //         if(memcmp(line_bytes, version_bytes, sizeof(version)) != 0) {
//             //             return false;
//             //         }
//             //         check_start += sizeof(version);
//             //         version_bytes += sizeof(version);
//             //         bytes_to_check -= sizeof(version);
//             //     } else {
//             //         // check bytes can be read
//             //         int bytes_can_check_this_loop = nextb - check_start;
//             //         if (bytes_can_check_this_loop > bytes_to_check) {
//             //             bytes_can_check_this_loop = bytes_to_check;
//             //         }
//             //         if(memcmp(line_bytes, version_bytes, bytes_can_check_this_loop) != 0) {
//             //             return false;
//             //         }
//             //         check_start += bytes_can_check_this_loop;
//             //         version_bytes += bytes_can_check_this_loop;
//             //         bytes_to_check -= bytes_can_check_this_loop;
//             //     }
//             // }

//             cache.unlock(i);
//             i = nextb;
//         }
//     }
//     // epicAssert(bytes_to_check == 0);
//     return version == actual_version;
// }

bool Worker::ProcessLocalVersionCheck(WorkRequest* wr) {
#ifdef USE_LOCAL_VERSION_CHECK
    epicAssert(wr->addr);
    epicAssert(!(wr->flag & ASYNC));

    if (!(wr->flag & FENCE)) {
        Fence* fence = fences_.at(wr->fd);
        fence->lock();
        if (unlikely(IsMFenced(fence, wr))) {
            AddToFence(fence, wr);
            epicLog(LOG_DEBUG, "fenced (mfenced = %d, sfenced = %d): %d",
                    fence->mfenced, fence->sfenced, wr->op);
            fence->unlock();
            return FENCE_PENDING;
        }
        fence->unlock();
    }

    GAddr start = wr->addr;
    GAddr start_blk = TOBLOCK(start);
    GAddr end = GADD(start, wr->size);
    if (TOBLOCK(end - 1) != start_blk) {
        epicLog(LOG_INFO, "read/write split to multiple blocks");
    }
    GAddr check_start = end - sizeof(uint64_t);
    GAddr check_end = check_start + sizeof(uint64_t);
    uint64_t actual_version;
    // char* version_bytes = (char*)(&version);
    // int bytes_to_check = sizeof(version);
    if (IsLocal(start)) {
        // now assume the data doesn't cross nodes
        epicAssert(IsLocal(end));
        for (GAddr i = start_blk; i < end;) {
            GAddr nextb = BADD(i, 1);
            void* laddr = ToLocal(i);
            directory.lock(laddr);
            DirEntry* entry = directory.GetEntry(laddr);
            // epicAssert(entry);
            DirState state = directory.GetState(entry);
            if (directory.InTransitionState(state)) {
                epicLog(LOG_INFO,
                        "directory cache in transition state when local version check %d", entry->cache_state);
                AddToServeLocalRequest(i, wr);
                directory.unlock(laddr);
                // wr->unlock();
                return IN_TRANSITION;
            }
            // check state and lock
            if (entry && (entry->state == DIR_DIRTY || directory.IsWLocked(entry, laddr))) {
                *(wr->version_check_status) = VERSION_CHECK_FAILED;
                directory.unlock(laddr);
                break;
            }

            if (likely(check_start < nextb && check_end > i)) {
                GAddr gs = i > check_start ? i : check_start;
                void* dest = (void*)((ptr_t)(&actual_version) + GMINUS(gs, check_start));
                int len = nextb > check_end ? GMINUS(check_end, gs) : GMINUS(nextb, gs);
                memcpy(dest, ToLocal(gs), len);
            }

            directory.unlock(laddr);
            i = nextb;
        }
    } else {

        for (GAddr i = start_blk; i < end;) {
            GAddr nextb = BADD(i, 1);
            cache.lock(i);
            CacheLine* cline = cache.GetCLine(i);
            if (cache.InTransitionState(cline)) {
                epicLog(LOG_INFO,
                        "cache in transition state when local version check %d", cline->state);
                AddToServeLocalRequest(i, wr);
                cache.unlock(i);
                // wr->unlock();
                return IN_TRANSITION;
            }
            // check state and lock
            if (!cline || cache.IsWLocked(i) || cline->state == CACHE_INVALID) {
                *(wr->version_check_status) = VERSION_CHECK_FAILED;
                cache.unlock(i);
                break;
            }

            epicAssert(cline->state == CACHE_SHARED || cline->state == CACHE_DIRTY);

            if (likely(check_start < nextb && check_end > i)) {
                GAddr gs = i > check_start ? i : check_start;
                void* source = (void*)((ptr_t)(cline->line) + GMINUS(gs, i));
                void* dest = (void*)((ptr_t)(&actual_version) + GMINUS(gs, check_start));
                int len = nextb > check_end ? GMINUS(check_end, gs) : GMINUS(nextb, gs);
                memcpy(dest, source, len);
            }

            cache.unlock(i);
            i = nextb;
        }
    }
    if (*(wr->version_check_status) != VERSION_CHECK_FAILED) {
        *(wr->version_check_status) = wr->target_version == actual_version ? SUCCESS : VERSION_CHECK_FAILED;
    }

#ifdef MULTITHREAD
    if (wr->flag & TO_SERVE || wr->flag & FENCE) {
#endif
        /*
         * notify the app thread directly
         * this can only happen when the request can be fulfilled locally
         * or we don't need to wait for reply from remote node
         */
        if (Notify(wr)) {
            epicLog(LOG_WARNING, "cannot wake up the app thread");
        }
#ifdef MULTITHREAD
    } else {

    }
#endif
    return SUCCESS;
#endif
}
