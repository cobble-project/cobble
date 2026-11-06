#pragma once

#include <cstdint>

namespace cobble {

enum class ImportStorageMode : std::uint8_t {
  kAdoptAsync = 0,
  kReferencePersistent = 1,
  kReferencePersistentWithCache = 2,
};

}  // namespace cobble
