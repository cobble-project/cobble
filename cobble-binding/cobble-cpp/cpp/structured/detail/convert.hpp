#pragma once

#include <limits>

#include <cobble/structured.hpp>

#include "../../detail/convert.hpp"
#include "../../detail/error.hpp"
#include "bridge.hpp"

namespace cobble::structured::detail {

using cobble::detail::Translate;

structured_ffi::NativeWriteOptions
ToNative(const cobble::WriteOptions &options);
rust::Vec<structured_ffi::NativeBucketRange>
ToNativeRanges(std::span<const BucketRange> ranges);
rust::Vec<structured_ffi::NativeBytesDescriptor>
ToNativeElements(std::span<const BytesView> elements);
structured_ffi::NativeListConfig ToNative(const ListConfig &config);
Schema ToSchema(const structured_ffi::NativeStructuredSchema &native);
ShardSnapshot
ToShardSnapshot(const structured_ffi::NativeShardSnapshot &native);
GlobalSnapshot ToGlobalSnapshot(const structured_ffi::NativeSnapshot &native);
std::vector<MetricSample>
ToMetrics(rust::Vec<structured_ffi::NativeMetric> native);

inline rust::Slice<const Byte> RustBytes(BytesView value) noexcept {
  return cobble::detail::RustBytes(value);
}

inline rust::Str RustStr(std::string_view value) noexcept {
  return cobble::detail::RustStr(value);
}

inline BytesView ToView(rust::Slice<const Byte> value) noexcept {
  return cobble::detail::ToView(value);
}

} // namespace cobble::structured::detail
