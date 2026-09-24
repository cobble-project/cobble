#pragma once

#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include <cobble/reader.hpp>
#include <cobble/structured/multi_get.hpp>
#include <cobble/structured/options.hpp>
#include <cobble/structured/scan.hpp>
#include <cobble/structured/schema.hpp>

namespace cobble::structured {

using ReaderMode = cobble::ReaderMode;

class COBBLE_CPP_API Reader final {
 public:
  [[nodiscard]] static Reader OpenCurrent(std::string_view config_json);
  [[nodiscard]] static Reader OpenCurrentFile(std::string_view config_path);
  [[nodiscard]] static Reader Open(std::string_view config_json,
                                   SnapshotId global_snapshot);
  [[nodiscard]] static Reader OpenFile(std::string_view config_path,
                                       SnapshotId global_snapshot);
  // Uses the supplied fixed manifest directly; does not reload its global file.
  [[nodiscard]] static Reader Open(std::string_view config_json,
                                   const GlobalSnapshot &global_snapshot);
  [[nodiscard]] static Reader OpenFile(std::string_view config_path,
                                       const GlobalSnapshot &global_snapshot);

  Reader(Reader &&) noexcept;
  Reader &operator=(Reader &&) noexcept;
  ~Reader();
  Reader(const Reader &) = delete;
  Reader &operator=(const Reader &) = delete;

  // Data operations may auto-refresh and require external synchronization.
  // A snapshot-pinned Reader cannot Refresh.
  void Refresh();
  [[nodiscard]] OwnedRow Get(BucketId bucket, BytesView key,
                             const ReadOptions &options);
  [[nodiscard]] OwnedRow Get(BucketId bucket, BytesView key);
  [[nodiscard]] BufferResult GetInto(BucketId bucket, BytesView key,
                                     MutableBytesView output,
                                     const ReadOptions &options);
  [[nodiscard]] BufferResult GetInto(BucketId bucket, BytesView key,
                                     MutableBytesView output);
  [[nodiscard]] OwnedMultiGetResult MultiGet(std::span<const MultiGetKey> keys,
                                             const ReadOptions &options);
  [[nodiscard]] OwnedMultiGetResult MultiGet(std::span<const MultiGetKey> keys);
  [[nodiscard]] BufferResult MultiGetInto(std::span<const MultiGetKey> keys,
                                          MutableBytesView output,
                                          const ReadOptions &options);
  [[nodiscard]] BufferResult MultiGetInto(std::span<const MultiGetKey> keys,
                                          MutableBytesView output);
  // Core StructuredReader accepts only [start_inclusive, end_exclusive).
  [[nodiscard]] ScanCursor Scan(BucketId bucket, BytesView start_inclusive,
                                BytesView end_exclusive,
                                const ScanOptions &options = {});
  [[nodiscard]] Schema CurrentSchema() const;
  [[nodiscard]] ReaderMode Mode() const;
  [[nodiscard]] std::optional<SnapshotId> ConfiguredSnapshotId() const;
  [[nodiscard]] GlobalSnapshot CurrentGlobalSnapshot() const;
  [[nodiscard]] std::vector<GlobalSnapshot> ListGlobalSnapshots() const;

 private:
  struct Impl;
  explicit Reader(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cobble::structured
