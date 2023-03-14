#include "Meta.h"

#include <fcntl.h>

#include <cstdint>

namespace Database {
GAlloc** gallocators = NULL;
GAlloc* default_gallocator = NULL;
GAlloc* epoch_gallocator = NULL;
size_t gThreadCount = 0;
size_t gParamBatchSize = 1000;
int64_t recordnum = 0;
bool ok = false;
}  // namespace Database
