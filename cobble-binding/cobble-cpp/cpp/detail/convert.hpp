#pragma once

#include <cobble/cobble.hpp>

#include "bridge.hpp"

namespace cobble::detail {

const char* NonNullData(std::string_view value) noexcept;
rust::Slice<const Byte> RustBytes(BytesView value) noexcept;
rust::Slice<Byte> RustBytes(MutableBytesView value) noexcept;
rust::Str RustStr(std::string_view value) noexcept;
rust::String RustString(std::string_view value);
std::size_t ToSize(std::uint64_t value, std::string_view field);
BytesView ToView(rust::Slice<const Byte> value) noexcept;
BufferStatus ToBufferStatus(std::uint8_t status);
BufferResult ToBufferResult(const ffi::NativeBufferResult& native);
rust::Vec<ffi::NativeRange> ToNativeRanges(
    std::span<const BucketRange> ranges);
ShardSnapshot ToShardSnapshot(const ffi::NativeShardSnapshot& native);
GlobalSnapshot ToGlobalSnapshot(const ffi::NativeSnapshot& native);
ffi::NativeShardSnapshot ToNativeShardSnapshot(const ShardSnapshot& snapshot);
rust::Vec<ffi::NativeShardSnapshot> ToNativeShardSnapshots(
    std::span<const ShardSnapshot> snapshots);
ffi::NativeScanSplit ToNativeScanSplit(const ScanSplit& split);
ScanSplit ToScanSplit(const ffi::NativeScanSplit& split);
Schema ToSchema(const ffi::NativeSchema& native);
std::vector<MetricSample> ToMetrics(rust::Vec<ffi::NativeMetric> native);

}  // namespace cobble::detail
