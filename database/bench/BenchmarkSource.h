// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_BENCHMARK_SOURCE_H__
#define __DATABASE_BENCHMARK_SOURCE_H__

#include <cassert>
#include <iostream>
#include <sstream>
#include <string>

#include "BenchmarkArguments.h"
#include "IORedirector.h"
#include "Meta.h"
#include "TimeMeasurer.h"
#include "ycsb/YcsbTxnParams.h"

namespace Database {
class BenchmarkSource {
   public:
    BenchmarkSource(IORedirector* redirector, size_t num_txn,
                    size_t source_type, size_t thread_count, size_t dist_ratio,
                    std::string& source_filename = txn_data_filename)
        : redirector_ptr_(redirector),
          num_txn_(num_txn),
          source_type_(source_type),
          dist_ratio_(dist_ratio),
          thread_count_(thread_count),
          source_filename_(txn_data_filename) {}
    ~BenchmarkSource() {}

    void Start() {
        TimeMeasurer timer;
        timer.StartTimer();
        if (enable_checkpoint_reload) {
            // load from file.
            ReloadFromDisk();
        } else if (enable_checkpoint_save) {
            // generate params and dump to file.
            output_file_.open(source_filename_, std::ofstream::binary);
            // assert(output_file_.good() == true);
            StartGeneration();
            output_file_.close();
        } else {
            StartGeneration();
        }
        StartGeneration();
        timer.EndTimer();
        std::cout << "source elapsed time=" << timer.GetElapsedMilliSeconds()
                  << "ms" << std::endl;
    }

    virtual void StartGeneration() = 0;

   protected:
    void DumpToDisk(ParamBatch* tuples) {
        for (size_t i = 0; i < tuples->size(); ++i) {
            YcsbBenchmark::YcsbParam* tuple =
                reinterpret_cast<YcsbBenchmark::YcsbParam*>(tuples->get(i));
            CharArray param_chars;
            tuple->Serialize(param_chars);
            // write stored procedure type.
            size_t tuple_type = tuple->type_;
            output_file_.write((char*)(&tuple_type), sizeof(tuple_type));
            // write parameter size.
            output_file_.write((char*)(&param_chars.size_),
                               sizeof(param_chars.size_));
            // write parameters.
            output_file_.write(param_chars.char_ptr_, param_chars.size_);
            output_file_.flush();
            param_chars.Release();
        }
    }

   private:
    virtual void ReloadFromDisk() {
        std::ifstream log_reloader(source_filename_, std::ifstream::binary);
        assert(log_reloader.good() == true);
        log_reloader.seekg(0, std::ios::end);
        size_t file_size = static_cast<size_t>(log_reloader.tellg());
        log_reloader.seekg(0, std::ios::beg);
        size_t file_pos = 0;
        CharArray entry;
        entry.Allocate(10240);  // YcsbParam:5956->5380
        ParamBatch* tuples = new ParamBatch(gParamBatchSize);
        while (file_pos < file_size) {
            size_t param_type;
            log_reloader.read(reinterpret_cast<char*>(&param_type),
                              sizeof(param_type));
            file_pos += sizeof(param_type);
            log_reloader.read(reinterpret_cast<char*>(&entry.size_),
                              sizeof(entry.size_));
            file_pos += sizeof(entry.size_);
            if (file_size - file_pos >= entry.size_) {
                log_reloader.read(entry.char_ptr_, entry.size_);
                YcsbBenchmark::YcsbParam* event_tuple =
                    new YcsbBenchmark::YcsbParam();
                event_tuple->Deserialize(entry);
                if (event_tuple != NULL) {
                    tuples->push_back(event_tuple);
                    if (tuples->size() == gParamBatchSize) {
                        redirector_ptr_->PushParameterBatch(tuples);
                        tuples = new ParamBatch(gParamBatchSize);
                    }
                }
                file_pos += entry.size_;
            } else {
                break;
            }
        }
        if (tuples->size() != 0) {
            redirector_ptr_->PushParameterBatch(tuples);
        } else {
            delete tuples;
            tuples = NULL;
        }
        entry.Release();
        log_reloader.close();
    }

   protected:
    IORedirector* redirector_ptr_;
    const size_t num_txn_;
    const size_t dist_ratio_;
    const size_t source_type_;
    const size_t thread_count_;
    std::string source_filename_;
    std::ofstream output_file_;
};
}  // namespace Database

#endif
