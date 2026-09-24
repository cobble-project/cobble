#pragma once

#include <memory>
#include <span>
#include <string>
#include <string_view>

#include <cobble/structured/multi_get.hpp>
#include <cobble/structured/options.hpp>
#include <cobble/structured/scan.hpp>
#include <cobble/structured/schema.hpp>

namespace cobble::structured {

class COBBLE_CPP_API ReadOnlyDb final {
public:
  [[nodiscard]] static ReadOnlyDb Open(std::string_view config_json,
                                       SnapshotId snapshot,
                                       std::string_view source_db_id);
  [[nodiscard]] static ReadOnlyDb OpenFile(std::string_view config_path,
                                           SnapshotId snapshot,
                                           std::string_view source_db_id);

  ReadOnlyDb(ReadOnlyDb &&) noexcept;
  ReadOnlyDb &operator=(ReadOnlyDb &&) noexcept;
  ~ReadOnlyDb();
  ReadOnlyDb(const ReadOnlyDb &) = delete;
  ReadOnlyDb &operator=(const ReadOnlyDb &) = delete;

  [[nodiscard]] std::string Id() const;
  [[nodiscard]] OwnedRow Get(BucketId bucket, BytesView key,
                             const ReadOptions &options) const;
  [[nodiscard]] OwnedRow Get(BucketId bucket, BytesView key) const;
  [[nodiscard]] BufferResult GetInto(BucketId bucket, BytesView key,
                                     MutableBytesView output,
                                     const ReadOptions &options) const;
  [[nodiscard]] BufferResult GetInto(BucketId bucket, BytesView key,
                                     MutableBytesView output) const;
  [[nodiscard]] OwnedMultiGetResult MultiGet(std::span<const MultiGetKey> keys,
                                             const ReadOptions &options) const;
  [[nodiscard]] OwnedMultiGetResult
  MultiGet(std::span<const MultiGetKey> keys) const;
  [[nodiscard]] BufferResult MultiGetInto(std::span<const MultiGetKey> keys,
                                          MutableBytesView output,
                                          const ReadOptions &options) const;
  [[nodiscard]] BufferResult MultiGetInto(std::span<const MultiGetKey> keys,
                                          MutableBytesView output) const;
  [[nodiscard]] ScanCursor Scan(BucketId bucket, BytesView start_inclusive,
                                BytesView end_exclusive,
                                const ScanOptions &options = {}) const;
  [[nodiscard]] Schema CurrentSchema() const;

private:
  struct Impl;
  explicit ReadOnlyDb(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace cobble::structured
