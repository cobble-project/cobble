#include "detail/options.hpp"

#include <limits>

#include "detail/convert.hpp"

namespace cobble::detail {

ffi::NativeReadOptions ToNative(const ReadOptions& options) {
  ffi::NativeReadOptions native;
  if (options.column_family) {
    native.column_family = RustString(*options.column_family);
  }
  native.columns.reserve(options.columns.size());
  for (const auto column : options.columns) {
    native.columns.push_back(static_cast<std::uint64_t>(column));
  }
  return native;
}

ffi::NativeWriteOptions ToNative(const WriteOptions& options) {
  ffi::NativeWriteOptions native;
  native.has_ttl_seconds = options.ttl_seconds.has_value();
  native.ttl_seconds = options.ttl_seconds.value_or(0);
  if (options.column_family) {
    native.column_family = RustString(*options.column_family);
  }
  native.await_durable = options.await_durable;
  return native;
}

ffi::NativeScanOptions ToNative(const ScanOptions& options) {
  if (options.read_ahead_bytes > std::numeric_limits<std::uint64_t>::max()) {
    throw Error(ErrorCode::kInput,
                "scan read_ahead_bytes exceeds the supported size");
  }
  ffi::NativeScanOptions native;
  if (options.column_family) {
    native.column_family = RustString(*options.column_family);
  }
  native.columns.reserve(options.columns.size());
  for (const auto column : options.columns) {
    native.columns.push_back(static_cast<std::uint64_t>(column));
  }
  native.read_ahead_bytes = static_cast<std::uint64_t>(options.read_ahead_bytes);
  native.has_max_rows = options.max_rows.has_value();
  native.max_rows = options.max_rows.value_or(0);
  native.preload_scan_cursor_block = options.preload_scan_cursor_block;
  native.stop_at_block_boundary = options.stop_at_block_boundary;
  return native;
}

}  // namespace cobble::detail
