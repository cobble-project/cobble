#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

#include <cobble/structured/row.hpp>

namespace cobble::structured {

class COBBLE_CPP_API ScanOptions final {
public:
  ScanOptions();
  ScanOptions(const ScanOptions &);
  ScanOptions &operator=(const ScanOptions &);
  ScanOptions(ScanOptions &&) noexcept;
  ScanOptions &operator=(ScanOptions &&) noexcept;
  ~ScanOptions();

  ScanOptions &SetColumnFamily(std::optional<std::string_view> family);
  ScanOptions &SetColumns(std::span<const std::size_t> columns);
  ScanOptions &SetPreloadScanCursorBlock(bool enabled);
  ScanOptions &SetStopAtBlockBoundary(bool enabled);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  friend class Db;
  friend class SingleDb;
  friend struct ScanSplit;
};

class COBBLE_CPP_API OwnedBatch final {
public:
  OwnedBatch(OwnedBatch &&) noexcept;
  OwnedBatch &operator=(OwnedBatch &&) noexcept;
  ~OwnedBatch();
  OwnedBatch(const OwnedBatch &) = delete;
  OwnedBatch &operator=(const OwnedBatch &) = delete;

  [[nodiscard]] std::size_t RowCount() const noexcept;
  [[nodiscard]] bool End() const noexcept;
  [[nodiscard]] bool StoppedAtBlockBoundary() const noexcept;
  [[nodiscard]] BucketId Bucket(std::size_t row) const;
  [[nodiscard]] BytesView Key(std::size_t row) const;
  [[nodiscard]] std::size_t ColumnCount(std::size_t row) const;
  [[nodiscard]] bool HasColumn(std::size_t row, std::size_t column) const;
  [[nodiscard]] ColumnKind Kind(std::size_t row, std::size_t column) const;
  [[nodiscard]] BytesView Bytes(std::size_t row, std::size_t column) const;
  [[nodiscard]] std::size_t ListSize(std::size_t row, std::size_t column) const;
  [[nodiscard]] BytesView ListElement(std::size_t row, std::size_t column,
                                      std::size_t element) const;

private:
  struct Impl;
  explicit OwnedBatch(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
  friend class ScanCursor;
};

class COBBLE_CPP_API ScanCursor final {
public:
  ScanCursor(ScanCursor &&) noexcept;
  ScanCursor &operator=(ScanCursor &&) noexcept;
  ~ScanCursor();
  ScanCursor(const ScanCursor &) = delete;
  ScanCursor &operator=(const ScanCursor &) = delete;

  [[nodiscard]] OwnedBatch Next(std::size_t max_rows);
  [[nodiscard]] BufferResult NextBatchInto(std::size_t max_rows,
                                           MutableBytesView output);
  void ResumeAfterBlockBoundary();

private:
  struct Impl;
  explicit ScanCursor(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
  friend class Db;
  friend class SingleDb;
  friend struct ScanSplit;
};

} // namespace cobble::structured
