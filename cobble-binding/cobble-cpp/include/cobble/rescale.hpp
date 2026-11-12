#pragma once

#include <cstdint>

namespace cobble {

enum class ImportStorageMode : std::uint8_t {
  kAdoptAsync = 0,
  kReferencePersistent = 1,
  kReferencePersistentWithCache = 2,
};

struct RescaleOptions {
  bool snapshot_before = true;
  // If disabled, recovery sees the old topology until a later snapshot.
  // This does not disable automatic or asynchronous adoption safety snapshots.
  bool snapshot_after = true;
};

}  // namespace cobble
