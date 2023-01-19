#include "GamOperationProfiler.h"

// namespace Cavalia{
namespace Database {
std::unordered_map<size_t, long long> *gam_op_stat_;
#if defined(PRECISE_TIMER)
PreciseTimeMeasurer *gam_op_timer_;
#else
TimeMeasurer *gam_op_timer_;
#endif
}  // namespace Database
// }
