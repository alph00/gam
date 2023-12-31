#pragma once
#ifndef __CAVALIA_DATABASE_CC_ABORT_TIME_PROFILER_H__
#define __CAVALIA_DATABASE_CC_ABORT_TIME_PROFILER_H__

#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_CC_ABORT_TIME)
#if defined(PRECISE_TIMER)
#define INIT_CC_ABORT_TIME_PROFILER                                   \
    cc_abort_time_stat_ = new long long[gThreadCount];                \
    memset(cc_abort_time_stat_, 0, sizeof(long long) * gThreadCount); \
    cc_abort_timer_ = new PreciseTimeMeasurer[gThreadCount];

#else
#define INIT_CC_ABORT_TIME_PROFILER                                   \
    cc_abort_time_stat_ = new long long[gThreadCount];                \
    memset(cc_abort_time_stat_, 0, sizeof(long long) * gThreadCount); \
    cc_abort_timer_ = new TimeMeasurer[gThreadCount];
#endif

#define BEGIN_CC_ABORT_TIME_MEASURE(thread_id) \
    cc_abort_timer_[thread_id].StartTimer();

#define END_CC_ABORT_TIME_MEASURE(thread_id) \
    cc_abort_timer_[thread_id].EndTimer();   \
    cc_abort_time_stat_[thread_id] +=        \
        cc_abort_timer_[thread_id].GetElapsedNanoSeconds();

#define REPORT_CC_ABORT_TIME_PROFILER                                     \
    printf("**********BEGIN CC ABORT TIME PROFILING REPORT**********\n"); \
    long long cc_abort_time_total = 0;                                    \
    for (int i = 0; i < gThreadCount; ++i) {                              \
        if (cc_abort_time_stat_[i] != 0) {                                \
            cc_abort_time_total += cc_abort_time_stat_[i] / 1000 / 1000;  \
            printf("thread_id = %d, abort_time = %lld ms\n", i,           \
                   cc_abort_time_stat_[i] / 1000 / 1000);                 \
        }                                                                 \
    }                                                                     \
    printf("\navg abort time = %lld ms\n",                                \
           cc_abort_time_total / gThreadCount);                           \
    printf("**********END CC ABORT TIME PROFILING REPORT**********\n\n"); \
    delete[] cc_abort_time_stat_;                                         \
    cc_abort_time_stat_ = NULL;                                           \
    delete[] cc_abort_timer_;                                             \
    cc_abort_timer_ = NULL;

#else
#define INIT_CC_ABORT_TIME_PROFILER ;
#define BEGIN_CC_ABORT_TIME_MEASURE(thread_id) ;
#define END_CC_ABORT_TIME_MEASURE(thread_id) ;
#define REPORT_CC_ABORT_TIME_PROFILER ;
#endif

// namespace Cavalia{
namespace Database {
extern long long *cc_abort_time_stat_;
#if defined(PRECISE_TIMER)
extern PreciseTimeMeasurer *cc_abort_timer_;
#else
extern TimeMeasurer *cc_abort_timer_;
#endif
}  // namespace Database
// }

#endif
