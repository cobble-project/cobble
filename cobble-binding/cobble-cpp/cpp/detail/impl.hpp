#pragma once

#include <cobble/cobble.hpp>

#include "bridge.hpp"

namespace cobble {

struct OwnedRow::Impl {
  explicit Impl(rust::Box<ffi::NativeRow> native_row)
      : native(std::move(native_row)) {}
  rust::Box<ffi::NativeRow> native;
};

struct OwnedBatch::Impl {
  explicit Impl(rust::Box<ffi::NativeBatch> native_batch)
      : native(std::move(native_batch)) {}
  rust::Box<ffi::NativeBatch> native;
};

struct WriteBatch::Impl {
  Impl() : native(ffi::native_write_batch_new()) {}
  rust::Box<ffi::NativeWriteBatch> native;
};

struct ScanCursor::Impl {
  explicit Impl(rust::Box<ffi::NativeScanCursor> native_cursor)
      : native(std::move(native_cursor)) {}
  rust::Box<ffi::NativeScanCursor> native;
};

struct Database::Impl {
  explicit Impl(rust::Box<ffi::NativeDatabase> native_database)
      : native(std::move(native_database)) {}
  rust::Box<ffi::NativeDatabase> native;
};

struct Db::Impl {
  explicit Impl(rust::Box<ffi::NativeShardedDatabase> native_database)
      : native(std::move(native_database)) {}
  rust::Box<ffi::NativeShardedDatabase> native;
};

}  // namespace cobble
