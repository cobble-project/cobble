#pragma once

#include <cstdint>

namespace cobble {

enum class MemtableType : std::uint8_t {
  kHash = 0,
  kSkiplist = 1,
  kVec = 2,
  kAdaptive = 3,
};

}  // namespace cobble
