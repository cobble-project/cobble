#pragma once

#include <cstddef>
#include <memory>

#include <cobble/types.hpp>

namespace cobble {

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
  friend class Db;
  friend class ReadOnlyDb;
  friend class Reader;
  friend struct ScanSplit;
};

}  // namespace cobble
