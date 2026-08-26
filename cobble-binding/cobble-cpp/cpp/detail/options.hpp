#pragma once

#include <cobble/options.hpp>

#include "bridge.hpp"

namespace cobble::detail {

ffi::NativeReadOptions ToNative(const ReadOptions& options);
ffi::NativeWriteOptions ToNative(const WriteOptions& options);
ffi::NativeScanOptions ToNative(const ScanOptions& options);

}  // namespace cobble::detail
