#pragma once

#include <chrono>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include <cobble/lifecycle.hpp>
#include <cobble/metrics.hpp>
#include <cobble/options.hpp>
#include <cobble/rescale.hpp>
#include <cobble/snapshot.hpp>
#include <cobble/structured/lifecycle.hpp>
#include <cobble/structured/multi_get.hpp>
#include <cobble/structured/options.hpp>
#include <cobble/structured/priority_queue.hpp>
#include <cobble/structured/row.hpp>
#include <cobble/structured/scan.hpp>
#include <cobble/structured/schema.hpp>
#include <cobble/structured/write_batch.hpp>

namespace cobble::structured {

class COBBLE_CPP_API Db final {
public:
  [[nodiscard]] static Db Open(std::string_view config_json);
  [[nodiscard]] static Db Open(std::string_view config_json,
                               std::span<const BucketRange> ranges);
  [[nodiscard]] static Db OpenFile(std::string_view config_path);
  [[nodiscard]] static Db OpenFile(std::string_view config_path,
                                   std::span<const BucketRange> ranges);
  [[nodiscard]] static Db
  OpenFromSnapshot(std::string_view config_json, SnapshotId snapshot,
                   std::string_view existing_db_id,
                   RecoveryMode mode = RecoveryMode::kSnapshotOnly);
  [[nodiscard]] static Db
  OpenFromSnapshotFile(std::string_view config_path, SnapshotId snapshot,
                       std::string_view existing_db_id,
                       RecoveryMode mode = RecoveryMode::kSnapshotOnly);
  [[nodiscard]] static Db RestoreNew(std::string_view config_json,
                                     SnapshotId source_snapshot,
                                     std::string_view source_db_id);
  [[nodiscard]] static Db RestoreNewFile(std::string_view config_path,
                                         SnapshotId source_snapshot,
                                         std::string_view source_db_id);
  [[nodiscard]] static Db
  RestoreNewFromManifest(std::string_view config_json,
                         std::string_view manifest_path);
  [[nodiscard]] static Db
  RestoreNewFromManifestFile(std::string_view config_path,
                             std::string_view manifest_path);
  [[nodiscard]] static Db
  Resume(std::string_view config_json, std::string_view existing_db_id,
         RecoveryMode mode = RecoveryMode::kLatestWithWal);
  [[nodiscard]] static Db
  ResumeFile(std::string_view config_path, std::string_view existing_db_id,
             RecoveryMode mode = RecoveryMode::kLatestWithWal);
  [[nodiscard]] static Db
  ResumeFromSnapshot(std::string_view config_json, SnapshotId snapshot,
                     std::string_view existing_db_id,
                     RecoveryMode mode = RecoveryMode::kSnapshotOnly);
  [[nodiscard]] static Db
  ResumeFromSnapshotFile(std::string_view config_path, SnapshotId snapshot,
                         std::string_view existing_db_id,
                         RecoveryMode mode = RecoveryMode::kSnapshotOnly);

  Db(Db &&) noexcept;
  Db &operator=(Db &&) noexcept;
  ~Db();
  Db(const Db &) = delete;
  Db &operator=(const Db &) = delete;

  [[nodiscard]] std::string Id() const;
  void PutBytes(BucketId bucket, BytesView key, ColumnIndex column,
                BytesView value,
                const cobble::WriteOptions &options = {}) const;
  void PutList(BucketId bucket, BytesView key, ColumnIndex column,
               std::span<const BytesView> elements,
               const cobble::WriteOptions &options = {}) const;
  void MergeBytes(BucketId bucket, BytesView key, ColumnIndex column,
                  BytesView value,
                  const cobble::WriteOptions &options = {}) const;
  void MergeList(BucketId bucket, BytesView key, ColumnIndex column,
                 std::span<const BytesView> elements,
                 const cobble::WriteOptions &options = {}) const;
  void Delete(BucketId bucket, BytesView key, ColumnIndex column,
              const cobble::WriteOptions &options = {}) const;
  [[nodiscard]] OwnedRow Get(BucketId bucket, BytesView key,
                             const ReadOptions &options) const;
  [[nodiscard]] OwnedRow Get(BucketId bucket, BytesView key) const;
  [[nodiscard]] BufferResult GetInto(BucketId bucket, BytesView key,
                                     MutableBytesView output,
                                     const ReadOptions &options) const;
  [[nodiscard]] BufferResult GetInto(BucketId bucket, BytesView key,
                                     MutableBytesView output) const;
  void Write(std::span<const WriteOperation> operations) const;
  void Write(WriteBatch &batch) const;
  [[nodiscard]] OwnedMultiGetResult MultiGet(std::span<const MultiGetKey> keys,
                                             const ReadOptions &options) const;
  [[nodiscard]] OwnedMultiGetResult
  MultiGet(std::span<const MultiGetKey> keys) const;
  [[nodiscard]] BufferResult MultiGetInto(std::span<const MultiGetKey> keys,
                                          MutableBytesView output,
                                          const ReadOptions &options) const;
  [[nodiscard]] BufferResult MultiGetInto(std::span<const MultiGetKey> keys,
                                          MutableBytesView output) const;
  [[nodiscard]] ScanCursor
  Scan(BucketId bucket, std::optional<BytesView> start_inclusive = std::nullopt,
       std::optional<BytesView> end_exclusive = std::nullopt,
       const ScanOptions &options = {}) const;
  [[nodiscard]] PriorityQueue NewPriorityQueue(std::string_view name);
  [[nodiscard]] PriorityQueue GetPriorityQueue(std::string_view name) const;
  [[nodiscard]] PriorityQueue GetOrNewPriorityQueue(std::string_view name);

  [[nodiscard]] Schema CurrentSchema() const;
  [[nodiscard]] SchemaBuilder UpdateSchema() const;
  void SetTime(std::uint32_t unix_seconds) const;
  [[nodiscard]] std::uint32_t NowSeconds() const;
  [[nodiscard]] SnapshotId Snapshot() const;
  [[nodiscard]] PendingShardSnapshot StartSnapshot() const;
  [[nodiscard]] ShardSnapshot TakeSnapshot() const;
  [[nodiscard]] bool CancelSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] ShardSnapshot GetShardSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] bool RetainSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] bool ExpireSnapshot(SnapshotId snapshot) const;
  void SwitchToSnapshot(SnapshotId snapshot);
  [[nodiscard]] std::vector<MetricSample> Metrics() const;
  void SwitchMemtableType(MemtableType type, bool flush_current) const;
  [[nodiscard]] std::size_t LoadReadonlyFilesToPrimary() const;
  [[nodiscard]] SnapshotId ExpandBucket(
      std::string_view source_db_id,
      std::optional<SnapshotId> source_snapshot = std::nullopt,
      std::optional<std::span<const BucketRange>> ranges = std::nullopt,
      ExpandStorageMode storage_mode = ExpandStorageMode::kAdoptAsync) const;
  void WaitForExpandAdoption(std::chrono::milliseconds timeout) const;
  [[nodiscard]] SnapshotId
  ShrinkBucket(std::span<const BucketRange> ranges) const;
  void Close() const;

private:
  struct Impl;
  explicit Db(std::shared_ptr<Impl>) noexcept;
  std::shared_ptr<Impl> impl_;

  friend class SchemaBuilder;
  friend class ScanCursor;
  friend class PriorityQueue;
};

} // namespace cobble::structured
