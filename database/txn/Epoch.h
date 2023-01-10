#pragma once
#ifndef __CAVALIA_DATABASE_EPOCH_H__
#define __CAVALIA_DATABASE_EPOCH_H__

#include <cstdint>
#include <boost/thread.hpp>
#include "gallocator.h"

#if defined(__linux__)
#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")
#else
#define COMPILER_MEMORY_FENCE
#endif

namespace Database {
	class Epoch {
	public:
		// Epoch() : 
		// 	epoch_addr_(Gnullptr), ts_thread_(NULL), is_master_(false), epoch_gallocator_(NULL){
		// 		// empty
		// }
		Epoch() = delete;

		Epoch(GAddr epoch_addr, bool is_master, GAlloc* epoch_gallocator_) : 
			epoch_addr_(epoch_addr), ts_thread_(NULL), is_master_(is_master), epoch_gallocator_(epoch_gallocator_) {
#if defined(OCC) || defined(SILO)
			if (is_master) {
				ts_thread_ = new boost::thread(boost::bind(&Epoch::Start, this));
			}
#endif
		}

		~Epoch() {
			if (ts_thread_) {
				delete ts_thread_;
				ts_thread_ = NULL;
			}
		}

		// called by each node/thread with its own gallocator
		uint64_t GetEpoch(GAlloc* gallocator) {
			gallocator->RLock(epoch_addr_, sizeof(uint64_t));
			uint64_t cur_epoch;
			gallocator->Read(epoch_addr_, &cur_epoch, sizeof(uint64_t));
			gallocator->UnLock(epoch_addr_, sizeof(uint64_t));
			return cur_epoch;
		}

	private:
		void Start() {
			while (true) {
				boost::this_thread::sleep(boost::posix_time::milliseconds(40));
				// TODO(weihaosun): use GFUNC instead of rmw
				epoch_gallocator_->WLock(epoch_addr_, sizeof(uint64_t));
				uint64_t cur_epoch;
				epoch_gallocator_->Read(epoch_addr_, &cur_epoch, sizeof(uint64_t));
				cur_epoch += 1;
				epoch_gallocator_->Write(epoch_addr_, &cur_epoch, sizeof(cur_epoch));
				epoch_gallocator_->UnLock(epoch_addr_, sizeof(uint64_t));
			}
		}

	private:
		// static volatile uint64_t curr_epoch_;
		GAddr epoch_addr_;
		boost::thread *ts_thread_;
		bool is_master_;
		GAlloc* epoch_gallocator_;
	};
}


#endif
