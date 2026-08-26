#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <cobble/options.hpp>
#include <cobble/scan.hpp>
#include <cobble/types.hpp>
#include <cobble/write_batch.hpp>

namespace cobble {

class COBBLE_CPP_API OwnedRow final {
 public:
  OwnedRow(OwnedRow&&) noexcept;
  OwnedRow& operator=(OwnedRow&&) noexcept;
  ~OwnedRow();

  OwnedRow(const OwnedRow&) = delete;
  OwnedRow& operator=(const OwnedRow&) = delete;

  [[nodiscard]] bool found() const noexcept;
  [[nodiscard]] std::size_t column_count() const noexcept;
  [[nodiscard]] bool has_column(std::size_t column) const;
  // The view remains valid while this OwnedRow is alive and is not moved.
  [[nodiscard]] BytesView column(std::size_t column) const;

 private:
  struct Impl;
  explicit OwnedRow(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class Database;
};

class COBBLE_CPP_API Database final {
 public:
  [[nodiscard]] static Database Open(std::string_view config_json);
  [[nodiscard]] static Database OpenFile(std::string_view config_path);
  [[nodiscard]] static Database Resume(std::string_view config_json,
                                       SnapshotId snapshot,
                                       RecoveryMode mode =
                                           RecoveryMode::kSnapshotOnly);
  [[nodiscard]] static Database ResumeFile(
      std::string_view config_path, SnapshotId snapshot,
      RecoveryMode mode = RecoveryMode::kSnapshotOnly);

  Database(Database&&) noexcept;
  Database& operator=(Database&&) noexcept;
  ~Database();

  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

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

  [[nodiscard]] ScanCursor Scan(
      BucketId bucket, std::optional<BytesView> start_inclusive,
      std::optional<BytesView> end_exclusive,
      const ScanOptions& options = {}) const;

  [[nodiscard]] SnapshotId Snapshot() const;
  [[nodiscard]] bool RetainSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] bool ExpireSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] std::vector<SnapshotId> ListSnapshots() const;
  [[nodiscard]] std::string SnapshotManifestJson(SnapshotId snapshot) const;

  void SetTime(std::uint32_t unix_seconds) const;
  void Close() const;

 private:
  struct Impl;
  explicit Database(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

[[nodiscard]] COBBLE_CPP_API std::string_view Version() noexcept;

}  // namespace cobble
