#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <cobble/types.hpp>

namespace cobble {

struct BucketRange {
  BucketId start_inclusive;
  BucketId end_inclusive;
};

struct ColumnFamilyId {
  std::string name;
  std::uint8_t id;
};

struct ShardSnapshot {
  std::vector<BucketRange> ranges;
  std::vector<ColumnFamilyId> column_families;
  std::string db_id;
  SnapshotId snapshot_id;
  std::string manifest_path;
  std::uint32_t timestamp_seconds;
  std::uint64_t data_size_bytes;
  std::uint64_t incremental_data_size_bytes;
};

struct GlobalSnapshot {
  std::uint32_t version;
  SnapshotId id;
  std::uint32_t total_buckets;
  std::vector<ColumnFamilyId> column_families;
  std::vector<ShardSnapshot> shards;
  std::uint32_t watermark_seconds;
};

class COBBLE_CPP_API PendingSnapshot final {
 public:
  PendingSnapshot(PendingSnapshot&&) noexcept;
  PendingSnapshot& operator=(PendingSnapshot&&) noexcept;
  ~PendingSnapshot();
  PendingSnapshot(const PendingSnapshot&) = delete;
  PendingSnapshot& operator=(const PendingSnapshot&) = delete;
  [[nodiscard]] SnapshotId id() const noexcept;
  // Single-consumer blocking wait. Destruction does not cancel the snapshot.
  [[nodiscard]] GlobalSnapshot Wait();

 private:
  struct Impl;
  explicit PendingSnapshot(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
  friend class Database;
};

class COBBLE_CPP_API PendingShardSnapshot final {
 public:
  PendingShardSnapshot(PendingShardSnapshot&&) noexcept;
  PendingShardSnapshot& operator=(PendingShardSnapshot&&) noexcept;
  ~PendingShardSnapshot();

  PendingShardSnapshot(const PendingShardSnapshot&) = delete;
  PendingShardSnapshot& operator=(const PendingShardSnapshot&) = delete;

  [[nodiscard]] SnapshotId id() const noexcept;
  // Single-consumer blocking wait. Destruction does not cancel the snapshot.
  [[nodiscard]] ShardSnapshot Wait();

 private:
  struct Impl;
  explicit PendingShardSnapshot(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class Db;
};

}  // namespace cobble
