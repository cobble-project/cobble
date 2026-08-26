#include "detail/convert.hpp"

#include <limits>

namespace cobble::detail {

const Byte* NonNullData(BytesView value) noexcept {
  static constexpr Byte kEmpty = 0;
  return value.empty() ? &kEmpty : value.data();
}

Byte* NonNullData(MutableBytesView value) noexcept {
  static Byte empty = 0;
  return value.empty() ? &empty : value.data();
}

const char* NonNullData(std::string_view value) noexcept {
  static constexpr char kEmpty = '\0';
  return value.empty() ? &kEmpty : value.data();
}

rust::Slice<const Byte> RustBytes(BytesView value) noexcept {
  return {NonNullData(value), value.size()};
}

rust::Slice<Byte> RustBytes(MutableBytesView value) noexcept {
  return {NonNullData(value), value.size()};
}

rust::Str RustStr(std::string_view value) noexcept {
  return {NonNullData(value), value.size()};
}

rust::String RustString(std::string_view value) {
  return {NonNullData(value), value.size()};
}

std::size_t ToSize(std::uint64_t value, std::string_view field) {
  if (value > std::numeric_limits<std::size_t>::max()) {
    throw Error(ErrorCode::kInvalidState,
                std::string(field) + " exceeds the C++ address space");
  }
  return static_cast<std::size_t>(value);
}

BytesView ToView(rust::Slice<const Byte> value) noexcept {
  return {value.data(), value.size()};
}

BufferStatus ToBufferStatus(std::uint8_t status) {
  switch (status) {
    case 0:
      return BufferStatus::kOk;
    case 1:
      return BufferStatus::kNotFound;
    case 2:
      return BufferStatus::kEnd;
    case 3:
      return BufferStatus::kBufferTooSmall;
    case 4:
      return BufferStatus::kBlockBoundary;
    default:
      throw Error(ErrorCode::kInvalidState,
                  "Rust bridge returned an unknown buffer status");
  }
}

BufferResult ToBufferResult(const ffi::NativeBufferResult& native) {
  return {ToBufferStatus(native.status), ToSize(native.bytes_written, "bytes_written"),
          ToSize(native.bytes_required, "bytes_required"),
          ToSize(native.row_count, "row_count")};
}

rust::Vec<ffi::NativeRange> ToNativeRanges(
    std::span<const BucketRange> ranges) {
  rust::Vec<ffi::NativeRange> native;
  native.reserve(ranges.size());
  for (const auto& range : ranges) {
    ffi::NativeRange value;
    value.first = range.start_inclusive;
    value.last = range.end_inclusive;
    native.push_back(value);
  }
  return native;
}

namespace {

rust::Vec<Byte> ToNativeBytes(const std::vector<Byte>& bytes) {
  rust::Vec<Byte> native;
  native.reserve(bytes.size());
  for (const auto byte : bytes) {
    native.push_back(byte);
  }
  return native;
}

std::vector<Byte> ToBytes(const rust::Vec<Byte>& bytes) {
  return {bytes.begin(), bytes.end()};
}

}  // namespace

ffi::NativeShardSnapshot ToNativeShardSnapshot(const ShardSnapshot& snapshot) {
  ffi::NativeShardSnapshot native;
  native.ranges = ToNativeRanges(snapshot.ranges);
  native.families.reserve(snapshot.column_families.size());
  for (const auto& family : snapshot.column_families) {
    ffi::NativeFamily value;
    value.name = RustString(family.name);
    value.id = family.id;
    native.families.push_back(std::move(value));
  }
  native.db_id = RustString(snapshot.db_id);
  native.snapshot_id = snapshot.snapshot_id;
  native.manifest_path = RustString(snapshot.manifest_path);
  native.timestamp_seconds = snapshot.timestamp_seconds;
  native.data_size_bytes = snapshot.data_size_bytes;
  native.incremental_data_size_bytes = snapshot.incremental_data_size_bytes;
  return native;
}

rust::Vec<ffi::NativeShardSnapshot> ToNativeShardSnapshots(
    std::span<const ShardSnapshot> snapshots) {
  rust::Vec<ffi::NativeShardSnapshot> native;
  native.reserve(snapshots.size());
  for (const auto& snapshot : snapshots) {
    native.push_back(ToNativeShardSnapshot(snapshot));
  }
  return native;
}

ffi::NativeScanSplit ToNativeScanSplit(const ScanSplit& split) {
  ffi::NativeScanSplit native;
  native.shard = ToNativeShardSnapshot(split.shard);
  native.has_start = split.start_inclusive.has_value();
  native.start = split.start_inclusive ? ToNativeBytes(*split.start_inclusive)
                                      : rust::Vec<Byte>();
  native.has_end = split.end_exclusive.has_value();
  native.end = split.end_exclusive ? ToNativeBytes(*split.end_exclusive)
                                   : rust::Vec<Byte>();
  native.has_start_after = split.start_after_exclusive.has_value();
  if (split.start_after_exclusive) {
    native.start_after_bucket = split.start_after_exclusive->bucket;
    native.start_after_key = ToNativeBytes(split.start_after_exclusive->key);
  }
  native.has_end_at = split.end_at_inclusive.has_value();
  if (split.end_at_inclusive) {
    native.end_at_bucket = split.end_at_inclusive->bucket;
    native.end_at_key = ToNativeBytes(split.end_at_inclusive->key);
  }
  return native;
}

ScanSplit ToScanSplit(const ffi::NativeScanSplit& native) {
  ScanSplit split;
  split.shard = ToShardSnapshot(native.shard);
  if (native.has_start) {
    split.start_inclusive = ToBytes(native.start);
  }
  if (native.has_end) {
    split.end_exclusive = ToBytes(native.end);
  }
  if (native.has_start_after) {
    split.start_after_exclusive =
        ScanSplitBoundary{native.start_after_bucket,
                          ToBytes(native.start_after_key)};
  }
  if (native.has_end_at) {
    split.end_at_inclusive =
        ScanSplitBoundary{native.end_at_bucket, ToBytes(native.end_at_key)};
  }
  return split;
}

}  // namespace cobble::detail
