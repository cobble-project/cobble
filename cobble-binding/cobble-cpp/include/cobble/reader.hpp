#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include <cobble/database.hpp>

namespace cobble {

enum class ReaderMode : std::uint8_t {
  kCurrent = 0,
  kSnapshot = 1,
};

class COBBLE_CPP_API Reader final {
 public:
  [[nodiscard]] static Reader OpenCurrent(std::string_view config_json);
  [[nodiscard]] static Reader OpenCurrentFile(std::string_view config_path);
  [[nodiscard]] static Reader Open(std::string_view config_json,
                                   SnapshotId global_snapshot);
  [[nodiscard]] static Reader OpenFile(std::string_view config_path,
                                       SnapshotId global_snapshot);

  Reader(Reader&&) noexcept;
  Reader& operator=(Reader&&) noexcept;
  ~Reader();

  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  // Reader maintains an LRU and may auto-refresh, so all data operations are
  // mutable and require external synchronization. Refresh is invalid for a
  // snapshot-pinned Reader.
  void Refresh();
  [[nodiscard]] OwnedRow Get(BucketId bucket, BytesView key,
                             const ReadOptions& options = {});
  [[nodiscard]] BufferResult GetColumnInto(
      BucketId bucket, BytesView key, MutableBytesView output,
      const ReadOptions& options);
  [[nodiscard]] OwnedMultiGetResult MultiGet(
      std::span<const MultiGetKey> keys,
      const ReadOptions& options = {});
  // Reader's core API is explicitly bounded: [start_inclusive, end_exclusive).
  [[nodiscard]] ScanCursor Scan(BucketId bucket, BytesView start_inclusive,
                                BytesView end_exclusive,
                                const ScanOptions& options = {});

  [[nodiscard]] ReaderMode Mode() const;
  [[nodiscard]] std::optional<SnapshotId> ConfiguredSnapshotId() const;
  [[nodiscard]] GlobalSnapshot CurrentGlobalSnapshot() const;
  [[nodiscard]] std::vector<GlobalSnapshot> ListGlobalSnapshots() const;

 private:
  struct Impl;
  explicit Reader(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cobble
