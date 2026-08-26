#pragma once

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <cobble/database.hpp>

namespace cobble {

class COBBLE_CPP_API ReadOnlyDb final {
 public:
  [[nodiscard]] static ReadOnlyDb Open(std::string_view config_json,
                                       SnapshotId snapshot,
                                       std::string_view source_db_id);
  [[nodiscard]] static ReadOnlyDb OpenFile(std::string_view config_path,
                                           SnapshotId snapshot,
                                           std::string_view source_db_id);

  ReadOnlyDb(ReadOnlyDb&&) noexcept;
  ReadOnlyDb& operator=(ReadOnlyDb&&) noexcept;
  ~ReadOnlyDb();

  ReadOnlyDb(const ReadOnlyDb&) = delete;
  ReadOnlyDb& operator=(const ReadOnlyDb&) = delete;

  [[nodiscard]] std::string Id() const;
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
  [[nodiscard]] std::vector<MetricSample> Metrics() const;

 private:
  struct Impl;
  explicit ReadOnlyDb(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cobble
