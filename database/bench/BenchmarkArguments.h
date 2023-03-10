// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_BENCHMARK_ARGUMENTS_H__
#define __DATABASE_BENCHMARK_ARGUMENTS_H__

#include <cassert>
#include <cstring>
#include <iostream>
#include <string>

#include "Meta.h"
#include "ycsb/YcsbParameter.h"

namespace Database {
static int app_type = -1;
static double scale_factors[2] = {-1, -1};
static int factor_count = 0;
static int dist_ratio = 1;
static int num_txn = -1;
static int num_core = -1;  // number of cores utilized in a single numa node.
static int port = -1;
static std::string config_filename = "config.txt";
// To modify tpcc workload
static size_t gReadRatio = 0;
static size_t gTimeLocality = 0;
static bool gForceRandomAccess = false;  // fixed
static bool gStandard = true;  // true if follow standard specification

// To modify ycsb workload
static double ycsb_theta = THETA;
static double ycsb_get_ratio = YCSB_GET_RATIO;
static double ycsb_put_ratio = YCSB_PUT_RATIO;
static double ycsb_update_ratio = YCSB_UPDATE_RATIO;
// To use checkpoint
static bool enable_checkpoint_reload = false;
static bool enable_checkpoint_save = false;
static std::string table_data_filename = "table_data";
static std::string txn_data_filename = "txn_data";

static void PrintUsage() {
    std::cout << "==========[USAGE]==========" << std::endl;
    std::cout << "\t-pINT: PORT(required)" << std::endl;
    std::cout << "\t-cINT: CORE_COUNT(required)" << std::endl;
    std::cout << "\t-sfINT: SCALE_FACTOR(required)" << std::endl;
    std::cout << "\t-sfDOUBLE: SCALE_FACTOR(required)" << std::endl;
    std::cout << "\t-tINT: TXN_COUNT(required)" << std::endl;
    std::cout << "\t-dINT: DIST_TXN_RATIO(optional,default=1)" << std::endl;
    // std::cout << "\t-zINT: BATCH_SIZE(optional)" << std::endl;
    std::cout << "\t-fSTRING: CONFIG_FILENAME(optional,default=config.txt)"
              << std::endl;
    std::cout << "\t-rINT: READ_RATIO(optional, [0,100])" << std::endl;
    std::cout << "\t-lINT: TIME_LOCALITY(optional, [0,100])" << std::endl;
    // ycsb
    std::cout << "\t-ythDOUBLE: YCSB_THETA(optional, [0,1],default=THETA(in "
                 "YcsbParameter.h) )"
              << std::endl;
    std::cout << "\t-ygetDOUBLE: YCSB_GET_RATIO(optional, "
                 "[0,1],default=YCSB_GET_RATIO(in "
                 "YcsbParameter.h) )"
              << std::endl;
    std::cout << "\t-yputDOUBLE: YCSB_PUT_RATIO(optional, "
                 "[0,1],default=YCSB_PUT_RATIO(in "
                 "YcsbParameter.h) )"
              << std::endl;
    std::cout << "\t-yupDOUBLE: YCSB_UPDATE_RATIO(optional, "
                 "[0,1],default=YCSB_UPDATE_RATIO(in "
                 "YcsbParameter.h) )"
              << std::endl;
    std::cout << "If ycsb, -sf,-d,-r,-l now don't work and "
                 "-yth、-yget、-yput、-yup works"
              << std::endl;
    // enable_checkpoint
    std::cout << "\t-ecpsINT: ENABLE_CHECKPOINT_SAVE(optional 0/1,default=0 )"
              << std::endl;
    std::cout << "\t-ecprINT: ENABLE_CHECKPOINT_RELOAD(optional 0/1,default=0 )"
              << std::endl;
    std::cout << "\t-ecpafSTRING: TABLE DATA FILE NAME(default=table_data )"
              << std::endl;
    std::cout << "\t-ecpxfSTRING: TXN DATA FILE NAME(default=txn_data )"
              << std::endl;

    std::cout << "\tnow only ycsb can enable checkpoint" << std::endl;
    std::cout << "===========================" << std::endl;
    std::cout << "==========[EXAMPLES]==========" << std::endl;
    std::cout << "Benchmark -p11111 -c4 -sf10 -sf100 -t100000" << std::endl;
    std::cout << "==============================" << std::endl;
}

static void ArgumentsChecker() {
    if (port == -1) {
        std::cout << "PORT (-p) should be set" << std::endl;
        exit(0);
    }
    if (factor_count == 0) {
        std::cout << "SCALE_FACTOR (-sf) should be set." << std::endl;
        exit(0);
    }
    if (num_core == -1) {
        std::cout << "CORE_COUNT (-c) should be set." << std::endl;
        exit(0);
    }
    if (num_txn == -1) {
        std::cout << "TXN_COUNT (-t) should be set." << std::endl;
        exit(0);
    }
    if (!(dist_ratio >= 0 && dist_ratio <= 100)) {
        std::cout << "DIST_TXN_RATIO should be [0,100]." << std::endl;
        exit(0);
    }
    if (!(gReadRatio >= 0 && gReadRatio <= 100)) {
        std::cout << "READ_RATIO should be [0,100]." << std::endl;
        exit(0);
    }
    if (!(gTimeLocality >= 0 && gTimeLocality <= 100)) {
        std::cout << "TIME_LOCALITY should be [0,100]." << std::endl;
        exit(0);
    }
    // for ycsb
    if (ycsb_get_ratio + ycsb_put_ratio + ycsb_update_ratio != 1) {
        std::cout << "the sum of 3 ratios(get、put、update) should be 1."
                  << std::endl;
        exit(0);
    }
}

static void ArgumentsParser(int argc, char *argv[]) {
    if (argc <= 4) {
        PrintUsage();
        exit(0);
    }
    for (int i = 1; i < argc; ++i) {
        if (argv[i][0] != '-') {
            PrintUsage();
            exit(0);
        }
        if (argv[i][1] == 'p') {
            port = atoi(&argv[i][2]);
        } else if (argv[i][1] == 's' && argv[i][2] == 'f') {
            scale_factors[factor_count] = atof(&argv[i][3]);
            ++factor_count;
        } else if (argv[i][1] == 't') {
            num_txn = atoi(&argv[i][2]);
        } else if (argv[i][1] == 'd') {
            dist_ratio = atoi(&argv[i][2]);
        } else if (argv[i][1] == 'c') {
            num_core = atoi(&argv[i][2]);
            gThreadCount = num_core;
        } else if (argv[i][1] == 'f') {
            config_filename = std::string(&argv[i][2]);
        } else if (argv[i][1] == 'z') {
            gParamBatchSize = atoi(&argv[i][2]);
        } else if (argv[i][1] == 'r') {
            gReadRatio = atoi(&argv[i][2]);
            gStandard = false;
        } else if (argv[i][1] == 'l') {
            gTimeLocality = atoi(&argv[i][2]);
            gStandard = false;
        } else if (argv[i][1] == 'h') {
            PrintUsage();
            exit(0);
        } else if (argv[i][1] == 'y' && argv[i][2] == 't' &&
                   argv[i][3] == 'h') {
            ycsb_theta = atof(&argv[i][4]);
        } else if (argv[i][1] == 'y' && argv[i][2] == 'g' &&
                   argv[i][3] == 'e' && argv[i][4] == 't') {
            ycsb_get_ratio = atof(&argv[i][5]);
        } else if (argv[i][1] == 'y' && argv[i][2] == 'p' &&
                   argv[i][3] == 'u' && argv[i][4] == 't') {
            ycsb_put_ratio = atof(&argv[i][5]);
        } else if (argv[i][1] == 'y' && argv[i][2] == 'u' &&
                   argv[i][3] == 'p') {
            ycsb_update_ratio = atof(&argv[i][4]);
        } else if (argv[i][1] == 'e' && argv[i][2] == 'c' &&
                   argv[i][3] == 'p' && argv[i][4] == 's') {
            enable_checkpoint_save = atoi(&argv[i][5]);
        } else if (argv[i][1] == 'e' && argv[i][2] == 'c' &&
                   argv[i][3] == 'p' && argv[i][4] == 'r') {
            enable_checkpoint_reload = atoi(&argv[i][5]);
        } else if (argv[i][1] == 'e' && argv[i][2] == 'c' &&
                   argv[i][3] == 'p' && argv[i][4] == 'a' &&
                   argv[i][5] == 'f') {
            table_data_filename = std::string(&argv[i][6]);
        } else if (argv[i][1] == 'e' && argv[i][2] == 'c' &&
                   argv[i][3] == 'p' && argv[i][4] == 'x' &&
                   argv[i][5] == 'f') {
            txn_data_filename = std::string(&argv[i][6]);
        } else {
            PrintUsage();
            exit(0);
        }
    }
    ArgumentsChecker();
}
}  // namespace Database

#endif
