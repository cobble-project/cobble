#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <cobble/database.hpp>
#include <cobble/rescale.hpp>

namespace cobble {

// Sharded raw bucket/key/column database. Unlike Database/SingleDb, callers may
// open explicit bucket ranges and change topology at runtime.
class COBBLE_CPP_API Db final {
 public:
  [[nodiscard]] static Db Open(std::string_view config_json);
  [[nodiscard]] static Db Open(std::string_view config_json,
                               std::span<const BucketRange> ranges);
  [[nodiscard]] static Db OpenFile(std::string_view config_path);
  [[nodiscard]] static Db OpenFile(std::string_view config_path,
                                   std::span<const BucketRange> ranges);

  [[nodiscard]] static Db OpenFromSnapshot(
      std::string_view config_json, SnapshotId snapshot,
      std::string_view existing_db_id,
      RecoveryMode mode = RecoveryMode::kSnapshotOnly);
  [[nodiscard]] static Db OpenFromSnapshotFile(
      std::string_view config_path, SnapshotId snapshot,
      std::string_view existing_db_id,
      RecoveryMode mode = RecoveryMode::kSnapshotOnly);
  [[nodiscard]] static Db RestoreNew(std::string_view config_json,
                                     SnapshotId source_snapshot,
                                     std::string_view source_db_id);
  [[nodiscard]] static Db RestoreNewFile(std::string_view config_path,
                                         SnapshotId source_snapshot,
                                         std::string_view source_db_id);
  [[nodiscard]] static Db RestoreNewFromManifest(
      std::string_view config_json, std::string_view manifest_path);
  [[nodiscard]] static Db RestoreNewFromManifestFile(
      std::string_view config_path, std::string_view manifest_path);
  // Resume selects the latest snapshot and replays its durable WAL tail by
  // default.
  [[nodiscard]] static Db Resume(
      std::string_view config_json, std::string_view existing_db_id,
      RecoveryMode mode = RecoveryMode::kLatestWithWal);
  [[nodiscard]] static Db ResumeFile(
      std::string_view config_path, std::string_view existing_db_id,
      RecoveryMode mode = RecoveryMode::kLatestWithWal);
  // ResumeFromSnapshot selects the exact snapshot boundary by default.
  [[nodiscard]] static Db ResumeFromSnapshot(
      std::string_view config_json, SnapshotId snapshot,
      std::string_view existing_db_id,
      RecoveryMode mode = RecoveryMode::kSnapshotOnly);
  [[nodiscard]] static Db ResumeFromSnapshotFile(
      std::string_view config_path, SnapshotId snapshot,
      std::string_view existing_db_id,
      RecoveryMode mode = RecoveryMode::kSnapshotOnly);

  Db(Db&&) noexcept;
  Db& operator=(Db&&) noexcept;
  ~Db();

  Db(const Db&) = delete;
  Db& operator=(const Db&) = delete;

  [[nodiscard]] std::string Id() const;

  void Put(BucketId bucket, BytesView key, ColumnIndex column,
           BytesView value, const WriteOptions& options = {}) const;
  void Delete(BucketId bucket, BytesView key, ColumnIndex column,
              const WriteOptions& options = {}) const;
  void Merge(BucketId bucket, BytesView key, ColumnIndex column,
             BytesView value, const WriteOptions& options = {}) const;
  void Write(WriteBatch batch, bool await_durable = true) const;

  [[nodiscard]] OwnedRow Get(BucketId bucket, BytesView key,
                             const ReadOptions& options = {}) const;
  [[nodiscard]] BufferResult GetColumnInto(
      BucketId bucket, BytesView key, MutableBytesView output,
      const ReadOptions& options) const;
  [[nodiscard]] OwnedMultiGetResult MultiGet(
      std::span<const MultiGetKey> keys,
      const ReadOptions& options = {}) const;
  [[nodiscard]] ScanCursor Scan(
      BucketId bucket, std::optional<BytesView> start_inclusive,
      std::optional<BytesView> end_exclusive,
      const ScanOptions& options = {}) const;

  [[nodiscard]] Schema CurrentSchema() const;
  [[nodiscard]] SchemaBuilder UpdateSchema() const;
  [[nodiscard]] std::vector<MetricSample> Metrics() const;
  void SetTime(std::uint32_t unix_seconds) const;
  [[nodiscard]] std::uint32_t NowSeconds() const;
  void SwitchMemtableType(MemtableType type, bool flush_current) const;
  [[nodiscard]] std::size_t LoadReadonlyFilesToPrimary() const;

  [[nodiscard]] SnapshotId Snapshot() const;
  [[nodiscard]] PendingShardSnapshot StartSnapshot() const;
  [[nodiscard]] ShardSnapshot TakeSnapshot() const;
  [[nodiscard]] bool CancelSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] ShardSnapshot GetShardSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] bool RetainSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] bool ExpireSnapshot(SnapshotId snapshot) const;

  // Controlled restart on this handle. This requires exclusive native owner
  // access: release all scan cursors and schema builders first. Cross-process
  // snapshot metadata mutation must also be externally serialized.
  void SwitchToSnapshot(SnapshotId snapshot);

  [[nodiscard]] SnapshotId ExpandBucket(
      std::string_view source_db_id,
      std::optional<SnapshotId> source_snapshot = std::nullopt,
      std::optional<std::span<const BucketRange>> ranges = std::nullopt,
      ExpandStorageMode storage_mode = ExpandStorageMode::kAdoptAsync) const;
  void WaitForExpandAdoption(std::chrono::milliseconds timeout) const;
  [[nodiscard]] SnapshotId ShrinkBucket(
      std::span<const BucketRange> ranges) const;

  void Close() const;

 private:
  struct Impl;
  explicit Db(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cobble
