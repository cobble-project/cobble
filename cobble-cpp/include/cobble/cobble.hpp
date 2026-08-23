#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#if defined(_WIN32) && !defined(COBBLE_CPP_STATIC)
#if defined(COBBLE_CPP_BUILDING_LIBRARY)
#define COBBLE_CPP_API __declspec(dllexport)
#else
#define COBBLE_CPP_API __declspec(dllimport)
#endif
#elif defined(__GNUC__) && !defined(COBBLE_CPP_STATIC)
#define COBBLE_CPP_API __attribute__((visibility("default")))
#else
#define COBBLE_CPP_API
#endif

namespace cobble {

using Byte = std::uint8_t;
using BytesView = std::span<const Byte>;
using MutableBytesView = std::span<Byte>;
using BucketId = std::uint16_t;
using ColumnIndex = std::uint16_t;
using SnapshotId = std::uint64_t;

enum class ErrorCode : std::uint8_t {
  kUnknown = 0,
  kUrl = 1,
  kFileSystem = 2,
  kIo = 3,
  kMemtableFull = 4,
  kConfiguration = 5,
  kInput = 6,
  kCoordination = 7,
  kInvalidState = 8,
  kFileFormat = 9,
  kChecksum = 10,
  kCancelled = 11,
};

class COBBLE_CPP_API Error final : public std::runtime_error {
 public:
  Error(ErrorCode code, std::string message);

  [[nodiscard]] ErrorCode code() const noexcept;

 private:
  ErrorCode code_;
};

enum class RecoveryMode : std::uint8_t {
  kSnapshotOnly = 0,
  kLatestWithWal = 1,
};

struct ReadOptions {
  std::optional<std::string> column_family;
  // Empty means all columns. Values are returned in this order.
  std::vector<std::size_t> columns;
};

struct WriteOptions {
  std::optional<std::uint32_t> ttl_seconds;
  std::optional<std::string> column_family;
  bool await_durable = true;
};

struct ScanOptions {
  std::optional<std::string> column_family;
  // Empty means all columns. Values are returned in this order.
  std::vector<std::size_t> columns;
  std::size_t read_ahead_bytes = 0;
  std::optional<std::size_t> max_rows;
  bool preload_scan_cursor_block = false;
  bool stop_at_block_boundary = false;
};

enum class BufferStatus : std::uint8_t {
  kOk = 0,
  kNotFound = 1,
  kEnd = 2,
  kBufferTooSmall = 3,
  kBlockBoundary = 4,
};

struct BufferResult {
  BufferStatus status = BufferStatus::kOk;
  std::size_t bytes_written = 0;
  std::size_t bytes_required = 0;
  std::size_t row_count = 0;
};

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

class COBBLE_CPP_API OwnedBatch final {
 public:
  OwnedBatch(OwnedBatch&&) noexcept;
  OwnedBatch& operator=(OwnedBatch&&) noexcept;
  ~OwnedBatch();

  OwnedBatch(const OwnedBatch&) = delete;
  OwnedBatch& operator=(const OwnedBatch&) = delete;

  [[nodiscard]] std::size_t row_count() const noexcept;
  [[nodiscard]] bool end() const noexcept;
  [[nodiscard]] bool stopped_at_block_boundary() const noexcept;
  [[nodiscard]] BucketId bucket(std::size_t row) const;
  // Returned views remain valid while this OwnedBatch is alive and is not moved.
  [[nodiscard]] BytesView key(std::size_t row) const;
  [[nodiscard]] std::size_t column_count(std::size_t row) const;
  [[nodiscard]] bool has_column(std::size_t row, std::size_t column) const;
  [[nodiscard]] BytesView column(std::size_t row, std::size_t column) const;

 private:
  struct Impl;
  explicit OwnedBatch(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class ScanCursor;
};

class COBBLE_CPP_API WriteBatch final {
 public:
  WriteBatch();
  WriteBatch(WriteBatch&&) noexcept;
  WriteBatch& operator=(WriteBatch&&) noexcept;
  ~WriteBatch();

  WriteBatch(const WriteBatch&) = delete;
  WriteBatch& operator=(const WriteBatch&) = delete;

  void Put(BucketId bucket, BytesView key, ColumnIndex column,
           BytesView value, const WriteOptions& options = {});
  void Delete(BucketId bucket, BytesView key, ColumnIndex column,
              const WriteOptions& options = {});
  void Merge(BucketId bucket, BytesView key, ColumnIndex column,
             BytesView value, const WriteOptions& options = {});
  [[nodiscard]] std::size_t size() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  friend class Database;
};

class COBBLE_CPP_API ScanCursor final {
 public:
  ScanCursor(ScanCursor&&) noexcept;
  ScanCursor& operator=(ScanCursor&&) noexcept;
  ~ScanCursor();

  ScanCursor(const ScanCursor&) = delete;
  ScanCursor& operator=(const ScanCursor&) = delete;

  // max_rows must be greater than zero.
  [[nodiscard]] OwnedBatch Next(std::size_t max_rows);
  // Writes the versioned Cobble row-batch encoding into caller-owned memory.
  // If the buffer is too small the cursor does not advance past the pending
  // batch; resize to bytes_required and call again.
  [[nodiscard]] BufferResult NextBatchInto(std::size_t max_rows,
                                           MutableBytesView output);
  void ResumeAfterBlockBoundary();

 private:
  struct Impl;
  explicit ScanCursor(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class Database;
};

class COBBLE_CPP_API Database final {
 public:
  // Opens a database from a partial or complete Cobble JSON configuration.
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
  // Reads one projected column into caller-owned memory. The projection must
  // select exactly one column. A too-small output is left unmodified.
  [[nodiscard]] BufferResult GetColumnInto(
      BucketId bucket, BytesView key, MutableBytesView output,
      const ReadOptions& options) const;

  [[nodiscard]] ScanCursor Scan(
      BucketId bucket, std::optional<BytesView> start_inclusive,
      std::optional<BytesView> end_exclusive,
      const ScanOptions& options = {}) const;

  // Snapshot creation is queued; the returned id can be inspected through the
  // manifest APIs after materialization completes.
  [[nodiscard]] SnapshotId Snapshot() const;
  [[nodiscard]] bool RetainSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] bool ExpireSnapshot(SnapshotId snapshot) const;
  [[nodiscard]] std::vector<SnapshotId> ListSnapshots() const;
  [[nodiscard]] std::string SnapshotManifestJson(SnapshotId snapshot) const;

  void SetTime(std::uint32_t unix_seconds) const;
  // Release all cursors before calling Close. Destruction alone is sufficient
  // for normal RAII use.
  void Close() const;

 private:
  struct Impl;
  explicit Database(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

[[nodiscard]] COBBLE_CPP_API std::string_view Version() noexcept;

}  // namespace cobble
