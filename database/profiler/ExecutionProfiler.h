#pragma once
#ifndef __CAVALIA_DATABASE_EXECUTION_PROFILER_H__
#define __CAVALIA_DATABASE_EXECUTION_PROFILER_H__

#include <unordered_map>
#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_EXECUTION)
#if defined(PRECISE_TIMER)
#define INIT_EXECUTION_PROFILER \
	execution_stat_ = new std::unordered_map<size_t, long long>[gThreadCount]; \
	execution_timer_ = new PreciseTimeMeasurer[gThreadCount];

#else
#define INIT_EXECUTION_PROFILER \
	execution_stat_ = new std::unordered_map<size_t, long long>[gThreadCount]; \
	execution_timer_ = new TimeMeasurer[gThreadCount];
#endif

#define BEGIN_TRANSACTION_TIME_MEASURE(thread_id) \
	execution_timer_[thread_id].StartTimer();

#define END_TRANSACTION_TIME_MEASURE(thread_id, txn_type) \
	execution_timer_[thread_id].EndTimer(); \
if (execution_stat_[thread_id].find(txn_type) == execution_stat_[thread_id].end()){ \
	execution_stat_[thread_id][txn_type] = execution_timer_[thread_id].GetElapsedNanoSeconds(); \
} \
else{ \
	execution_stat_[thread_id][txn_type] += execution_timer_[thread_id].GetElapsedNanoSeconds(); \
}

#define REPORT_EXECUTION_PROFILER \
	printf("**********BEGIN EXECUTION PROFILING REPORT**********\n"); \
	long long execution_time_total = 0; \
for (int i = 0; i < gThreadCount; ++i){ \
	std::map<size_t, long long> ordered_stat(execution_stat_[i].begin(), execution_stat_[i].end()); \
	long long cur_thread_execution_time = 0; \
for (auto &entry : ordered_stat){ \
	cur_thread_execution_time += entry.second / 1000 / 1000; \
	printf("thread_id = %d, txn_id = %d, execution_time = %lld ms\n", i, (int)entry.first, entry.second / 1000 / 1000); \
} \
	if (cur_thread_execution_time != 0) { \
		execution_time_total += cur_thread_execution_time; \
		printf("thread_id = %d, execution_time_total = %lld ms\n", i, cur_thread_execution_time); \
	} \
} \
	printf("\navg execution_time = %lld ms\n", execution_time_total / gThreadCount); \
	printf("**********END EXECUTION PROFILING REPORT**********\n\n"); \
	delete[] execution_stat_; \
	execution_stat_ = NULL; \
	delete[] execution_timer_; \
	execution_timer_ = NULL;

#else
#define INIT_EXECUTION_PROFILER ;
#define BEGIN_TRANSACTION_TIME_MEASURE(thread_id) ;
#define END_TRANSACTION_TIME_MEASURE(thread_id, txn_type) ;
#define REPORT_EXECUTION_PROFILER ;
#endif

// namespace Cavalia{
	namespace Database{
		extern std::unordered_map<size_t, long long> *execution_stat_;
#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer *execution_timer_;
#else
		extern TimeMeasurer *execution_timer_;
#endif
	}
// }

#endif
