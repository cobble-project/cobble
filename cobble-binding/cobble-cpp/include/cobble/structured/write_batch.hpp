#pragma once

#include <cstddef>
#include <memory>
#include <span>

#include <cobble/options.hpp>
#include <cobble/structured/types.hpp>

namespace cobble::structured {

enum class WriteOperationType : std::uint8_t {
  kPut = 0,
  kMerge = 1,
  kDelete = 2,
};

// Synchronous borrowed operation. Every view is required to remain valid only
// for the duration of Db::Write(span) / SingleDb::Write(span).
struct COBBLE_CPP_API WriteOperation {
  WriteOperationType operation = WriteOperationType::kPut;
  ColumnKind kind = ColumnKind::kBytes;
  BucketId bucket = 0;
  ColumnIndex column = 0;
  BytesView key;
  BytesView bytes_value;
  std::span<const BytesView> list_elements;
  cobble::WriteOptions options;

  [[nodiscard]] static WriteOperation
  PutBytes(BucketId bucket, BytesView key, ColumnIndex column, BytesView value,
           cobble::WriteOptions options = {});
  [[nodiscard]] static WriteOperation
  PutList(BucketId bucket, BytesView key, ColumnIndex column,
          std::span<const BytesView> elements,
          cobble::WriteOptions options = {});
  [[nodiscard]] static WriteOperation
  MergeBytes(BucketId bucket, BytesView key, ColumnIndex column,
             BytesView value, cobble::WriteOptions options = {});
  [[nodiscard]] static WriteOperation
  MergeList(BucketId bucket, BytesView key, ColumnIndex column,
            std::span<const BytesView> elements,
            cobble::WriteOptions options = {});
  [[nodiscard]] static WriteOperation Delete(BucketId bucket, BytesView key,
                                             ColumnIndex column,
                                             cobble::WriteOptions options = {});
};

// Reusable owning builder. Appending copies all byte payloads and metadata into
// C++-owned storage; source strings, spans, and containers may be destroyed
// before Write. Clear and successful Write retain internal capacities.
class COBBLE_CPP_API WriteBatch final {
public:
  WriteBatch();
  WriteBatch(WriteBatch &&) noexcept;
  WriteBatch &operator=(WriteBatch &&) noexcept;
  ~WriteBatch();
  WriteBatch(const WriteBatch &) = delete;
  WriteBatch &operator=(const WriteBatch &) = delete;

  void PutBytes(BucketId bucket, BytesView key, ColumnIndex column,
                BytesView value, const cobble::WriteOptions &options = {});
  void PutList(BucketId bucket, BytesView key, ColumnIndex column,
               std::span<const BytesView> elements,
               const cobble::WriteOptions &options = {});
  void MergeBytes(BucketId bucket, BytesView key, ColumnIndex column,
                  BytesView value, const cobble::WriteOptions &options = {});
  void MergeList(BucketId bucket, BytesView key, ColumnIndex column,
                 std::span<const BytesView> elements,
                 const cobble::WriteOptions &options = {});
  void Delete(BucketId bucket, BytesView key, ColumnIndex column,
              const cobble::WriteOptions &options = {});
  void Clear() noexcept;
  [[nodiscard]] std::size_t size() const noexcept;
  [[nodiscard]] bool empty() const noexcept;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  friend class Db;
  friend class SingleDb;
};

} // namespace cobble::structured
