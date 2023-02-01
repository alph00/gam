#ifndef __DATABASE_YCSB_RANDOM_GENERATOR_H__
#define __DATABASE_YCSB_RANDOM_GENERATOR_H__

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_set>

#include "YcsbConstants.h"

namespace Database {
namespace YcsbBenchmark {
class YcsbRandomGenerator {
   public:
    static int GenerateNonuniformInteger(const int &a, const int &x,
                                         const int &y) {
        int c = 0;
        if (a == 255) {
            c = cLast_;
        } else if (a == 1023) {
            c = cId_;
        } else if (a == 8191) {
            c = orderlineItemId_;
        }
        return (((GenerateInteger(0, a) | GenerateInteger(x, y)) + c) %
                (y - x + 1)) +
               x;
    }

    // generate integer that falls inside [min, max].
    static int GenerateInteger(const int &min, const int &max) {
        // TODO: needs a better, cross-platform random generator!
        if (RAND_MAX <= 32767) {
            return ((rand() << 16) + rand()) % (max - min + 1) + min;
        } else {
            return rand() % (max - min + 1) + min;
        }
    }

    // generate integer that falls inside [min, max).
    static int GenerateIntegerRightOpen(const int &min, const int &max) {
        // TODO: needs a better, cross-platform random generator!
        if (RAND_MAX <= 32767) {
            return ((rand() << 16) + rand()) % (max - min + 1) + min;
        } else {
            return rand() % (max - min) + min;
        }
    }

    // generate integer that falls inside [min, max] but not equal to excluding.
    static int GenerateIntegerExcluding(const int &min, const int &max,
                                        const int &excluding) {
        int number = GenerateInteger(min, max - 1);
        if (number >= excluding) {
            number += 1;
        }
        return number;
    }

    static double GenerateFixedPoint(const int &decimal_places,
                                     const double &minimum,
                                     const double &maximum) {
        int multiplier = 1;
        for (int i = 0; i < decimal_places; ++i) {
            multiplier *= 10;
        }

        int int_min = (int)(minimum * multiplier + 0.5);
        int int_max = (int)(maximum * multiplier + 0.5);

        return (double)(GenerateInteger(int_min, int_max) / (double)multiplier);
    }

    static double GenerateFixedPointRightOpen(const int &decimal_places,
                                              const double &minimum,
                                              const double &maximum) {
        int multiplier = 1;
        for (int i = 0; i < decimal_places; ++i) {
            multiplier *= 10;
        }

        int int_min = (int)(minimum * multiplier + 0.5);
        int int_max = (int)(maximum * multiplier + 0.5);

        return (double)(GenerateIntegerRightOpen(int_min, int_max) /
                        (double)multiplier);
    }

    // A random alphabetic string with length in range [minimum_length,
    // maximum_length].
    static std::string GenerateAString(const int &min_length,
                                       const int &max_length) {
        return GenerateRandomString(min_length, max_length, 'a', 26);
    }

    static std::string GenerateNString(const int &min_length,
                                       const int &max_length) {
        return GenerateRandomString(min_length, max_length, '0', 10);
    }

    static std::string GenerateRandomString(const int &min_length,
                                            const int &max_length,
                                            const char base,
                                            const int &num_characters) {
        int length = GenerateInteger(min_length, max_length);
        int base_byte = int(base);
        std::string str;
        str.resize(length);
        for (int i = 0; i < length; ++i) {
            str.at(i) =
                char(base_byte + GenerateInteger(0, num_characters - 1));
        }
        return str;
    }

    // A last name as defined by TPC-C 4.3.2.3. Not actually random. The input
    // number should fall into [0, 999].
    static std::string GenerateLastName(const int &number) {
        int index1 = number / 100;
        int index2 = (number / 10) % 10;
        int index3 = number % 10;
        return syllables_[index1] + syllables_[index2] + syllables_[index3];
    }

    // A non-uniform random last name, as defined by TPC-C 4.3.2.3. The name
    // will be limited to maxCID.
    static std::string GenerateRandomLastName(const int &max_cid) {
        int min_cid = 999;
        if (max_cid - 1 < min_cid) {
            min_cid = max_cid - 1;
        }
        return GenerateLastName(999);
        // TODO: require randomized last name.
        // return GenerateLastName(GenerateNonuniformInteger(255, 0, min_cid));
    }

    static void SelectUniqueIds(const int &numUnique, const int &minimum,
                                const int &maximum,
                                std::unordered_set<int> &rows) {
        for (int i = 0; i < numUnique; ++i) {
            int num = -1;
            while (num == -1 || rows.find(num) != rows.end()) {
                num = GenerateInteger(minimum, maximum);
            }
            rows.insert(num);
        }
    }

   private:
    const static std::string syllables_[10];
    const static int cLast_;
    const static int cId_;
    const static int orderlineItemId_;
};
}  // namespace YcsbBenchmark
}  // namespace Database

#endif
