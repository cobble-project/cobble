#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
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

struct SnapshotColumnFamily {
  std::string name;
  std::uint8_t id;
  std::size_t num_columns;
  bool value_has_ttl;
  std::string metadata_json;
};

struct COBBLE_CPP_API ShardSnapshot {
  std::vector<BucketRange> ranges;
  std::vector<ColumnFamilyId> column_families;
  std::string db_id;
  SnapshotId snapshot_id;
  std::string manifest_path;
  std::uint32_t timestamp_seconds;
  std::uint64_t data_size_bytes;
  std::uint64_t incremental_data_size_bytes;
  // Full DB reports include schema metadata; global-manifest references do not.
  bool has_schema_metadata = false;
  std::uint64_t schema_id = 0;
  std::vector<SnapshotColumnFamily> schema_column_families;

  [[nodiscard]] std::string ToJson() const;
  [[nodiscard]] static ShardSnapshot FromJson(std::string_view json);
};

struct COBBLE_CPP_API GlobalSnapshot {
  std::uint32_t version;
  SnapshotId id;
  std::uint32_t total_buckets;
  std::vector<ColumnFamilyId> column_families;
  std::vector<ShardSnapshot> shards;
  std::uint32_t watermark_seconds;

  [[nodiscard]] std::string ToJson() const;
  [[nodiscard]] static GlobalSnapshot FromJson(std::string_view json);
};

// Metadata-only reads: no DB, SST files, or coordinator are opened. Paths must
// be absolute and inside a configured metadata volume.
[[nodiscard]] COBBLE_CPP_API ShardSnapshot
LoadShardSnapshotMetadata(std::string_view config_json, std::string_view db_id,
                          std::string_view manifest_path);
[[nodiscard]] COBBLE_CPP_API ShardSnapshot LoadShardSnapshotMetadataFile(
    std::string_view config_path, std::string_view db_id,
    std::string_view manifest_path);
[[nodiscard]] COBBLE_CPP_API GlobalSnapshot LoadGlobalSnapshotMetadata(
    std::string_view config_json, std::string_view manifest_path);
[[nodiscard]] COBBLE_CPP_API GlobalSnapshot LoadGlobalSnapshotMetadataFile(
    std::string_view config_path, std::string_view manifest_path);

class COBBLE_CPP_API PendingSnapshot final {
 public:
  PendingSnapshot(PendingSnapshot&&) noexcept;
  PendingSnapshot& operator=(PendingSnapshot&&) noexcept;
  ~PendingSnapshot();
  PendingSnapshot(const PendingSnapshot&) = delete;
  PendingSnapshot& operator=(const PendingSnapshot&) = delete;
  [[nodiscard]] SnapshotId Id() const noexcept;
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

  [[nodiscard]] SnapshotId Id() const noexcept;
  // Single-consumer blocking wait. Destruction does not cancel the snapshot.
  [[nodiscard]] ShardSnapshot Wait();

 private:
  struct Impl;
  explicit PendingShardSnapshot(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class Db;
};

}  // namespace cobble
