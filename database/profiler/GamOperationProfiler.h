#pragma once
#ifndef __CAVALIA_DATABASE_GAM_OPERATION_PROFILER_H__
#define __CAVALIA_DATABASE_GAM_OPERATION_PROFILER_H__

#include <cstdio>
#include <map>
#include <unordered_map>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(GAM_OPERATION_PROFILE)
#if defined(PRECISE_TIMER)
#define INIT_GAM_OPERATION_PROFILER                                         \
    gam_op_stat_ = new std::unordered_map<size_t, long long>[gThreadCount]; \
    gam_op_timer_ = new PreciseTimeMeasurer[gThreadCount];
#else
#define INIT_GAM_OPERATION_PROFILER                                         \
    gam_op_stat_ = new std::unordered_map<size_t, long long>[gThreadCount]; \
    gam_op_timer_ = new TimeMeasurer[gThreadCount];
#endif

#define BEGIN_GAM_OPERATION_MEASURE(thread_id, gam_operation_type) \
    gam_op_timer_[thread_id].StartTimer();

#define END_GAM_OPERATION_MEASURE(thread_id, gam_operation_type) \
    gam_op_timer_[thread_id].EndTimer();                         \
    if (gam_op_stat_[thread_id].find(gam_operation_type) ==      \
        gam_op_stat_[thread_id].end()) {                         \
        gam_op_stat_[thread_id][gam_operation_type] =            \
            gam_op_timer_[thread_id].GetElapsedNanoSeconds();    \
    } else {                                                     \
        gam_op_stat_[thread_id][gam_operation_type] +=           \
            gam_op_timer_[thread_id].GetElapsedNanoSeconds();    \
    }

#define REPORT_GAM_OPERATION_PROFILER                                       \
    printf(                                                                 \
        "**********BEGIN GAM_OPERATION PROFILING REPORT(NOT INCLUDE INDEX " \
        "RELATED OPERATIONS)**********\n");                                 \
    std::map<size_t, long long> gam_op_time_total;                          \
    for (int i = 0; i < gThreadCount; ++i) {                                \
        std::map<size_t, long long> ordered_stat(gam_op_stat_[i].begin(),   \
                                                 gam_op_stat_[i].end());    \
        for (auto &entry : ordered_stat) {                                  \
            gam_op_time_total[entry.first] += entry.second / 1000 / 1000;   \
            printf(                                                         \
                "thread_id = %d, gam operation = %s, elapsed_time = %lld "  \
                "ms\n",                                                     \
                i, gam_operation_type_string[(int)entry.first].c_str(),     \
                entry.second / 1000 / 1000);                                \
        }                                                                   \
    }                                                                       \
    printf("\n");                                                           \
    for (auto &entry : gam_op_time_total) {                                 \
        printf("gam operation = %s, avg elapsed_time = %lld ms\n",          \
               gam_operation_type_string[(int)entry.first].c_str(),         \
               entry.second / gThreadCount);                                \
    }                                                                       \
    printf("**********END GAM_OPERATION PROFILING REPORT**********\n\n");   \
    delete[] gam_op_stat_;                                                  \
    gam_op_stat_ = NULL;                                                    \
    delete[] gam_op_timer_;                                                 \
    gam_op_timer_ = NULL;

#else
#define INIT_GAM_OPERATION_PROFILER ;
#define BEGIN_GAM_OPERATION_MEASURE(thread_id, gam_operation_type) ;
#define END_GAM_OPERATION_MEASURE(thread_id, gam_operation_type) ;
#define REPORT_GAM_OPERATION_PROFILER ;
#endif

namespace Database {
// enum PhaseType : size_t { INSERT_PHASE, SELECT_PHASE, COMMIT_PHASE,
// ABORT_PHASE };
enum GamOpType : size_t {
    GAM_READ,
    GAM_WRITE,
    GAM_RLOCK,
    GAM_WLOCK,
    GAM_TRY_RLOCK,
    GAM_TRY_WLOCK,
    GAM_UNLOCK,
    GAM_MALLOC,
    GAM_FREE,
    kGamOpCount
};

const static std::string gam_operation_type_string[kGamOpCount] = {
    "GAM_READ",      "GAM_WRITE",  "GAM_RLOCK",  "GAM_WLOCK", "GAM_TRY_RLOCK",
    "GAM_TRY_WLOCK", "GAM_UNLOCK", "GAM_MALLOC", "GAM_FREE"};

extern std::unordered_map<size_t, long long> *gam_op_stat_;
#if defined(PRECISE_TIMER)
extern PreciseTimeMeasurer *gam_op_timer_;
#else
extern TimeMeasurer *gam_op_timer_;
#endif
}  // namespace Database

#endif
