#pragma once

#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include <cobble/lifecycle.hpp>
#include <cobble/options.hpp>
#include <cobble/structured/lifecycle.hpp>
#include <cobble/structured/multi_get.hpp>
#include <cobble/structured/options.hpp>
#include <cobble/structured/priority_queue.hpp>
#include <cobble/structured/row.hpp>
#include <cobble/structured/scan.hpp>
#include <cobble/structured/schema.hpp>
#include <cobble/structured/write_batch.hpp>

namespace cobble::structured {

class COBBLE_CPP_API SingleDb final {
public:
  [[nodiscard]] static SingleDb Open(std::string_view config_json);
  [[nodiscard]] static SingleDb OpenFile(std::string_view config_path);

  SingleDb(SingleDb &&) noexcept;
  SingleDb &operator=(SingleDb &&) noexcept;
  ~SingleDb();
  SingleDb(const SingleDb &) = delete;
  SingleDb &operator=(const SingleDb &) = delete;

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
  [[nodiscard]] PendingSnapshot StartSnapshot() const;
  [[nodiscard]] GlobalSnapshot TakeSnapshot() const;
  [[nodiscard]] std::vector<GlobalSnapshot> ListSnapshots() const;
  [[nodiscard]] bool RetainSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] bool ExpireSnapshot(SnapshotId snapshot) const;
  void SwitchMemtableType(MemtableType type, bool flush_current) const;
  [[nodiscard]] std::size_t LoadReadonlyFilesToPrimary() const;
  void Close() const;

private:
  struct Impl;
  explicit SingleDb(std::shared_ptr<Impl>) noexcept;
  std::shared_ptr<Impl> impl_;

  friend class SchemaBuilder;
  friend class ScanCursor;
  friend class PriorityQueue;
};

} // namespace cobble::structured
