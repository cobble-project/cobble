#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include <cobble/snapshot.hpp>

namespace cobble {

class COBBLE_CPP_API DbCoordinator final {
 public:
  [[nodiscard]] static DbCoordinator Open(std::string_view config_json);
  [[nodiscard]] static DbCoordinator OpenFile(std::string_view config_path);

  DbCoordinator(DbCoordinator&&) noexcept;
  DbCoordinator& operator=(DbCoordinator&&) noexcept;
  ~DbCoordinator();

  DbCoordinator(const DbCoordinator&) = delete;
  DbCoordinator& operator=(const DbCoordinator&) = delete;

  // Requires the shard ranges to cover [0, total_buckets) exactly once.
  [[nodiscard]] GlobalSnapshot MaterializeGlobalSnapshot(
      std::uint32_t total_buckets, SnapshotId snapshot,
      std::span<const ShardSnapshot> shards) const;
  [[nodiscard]] GlobalSnapshot GetGlobalSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] std::vector<GlobalSnapshot> ListGlobalSnapshots() const;
  [[nodiscard]] std::optional<GlobalSnapshot> LoadCurrentGlobalSnapshot() const;
  // Retain protection is local to this coordinator instance.
  [[nodiscard]] bool RetainSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] bool ExpireSnapshot(SnapshotId snapshot) const;

 private:
  struct Impl;
  explicit DbCoordinator(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cobble
