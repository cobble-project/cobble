#pragma once

#include <memory>
#include <optional>
#include <variant>

#include <cobble/structured.hpp>

#include "bridge.hpp"

namespace cobble::structured {

struct Db::Impl {
  explicit Impl(rust::Box<structured_ffi::NativeStructuredDb> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativeStructuredDb> native;
};

struct SingleDb::Impl {
  explicit Impl(rust::Box<structured_ffi::NativeStructuredSingleDb> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativeStructuredSingleDb> native;
};

struct ReadOptions::Impl {
  explicit Impl(rust::Box<structured_ffi::NativeStructuredReadOptions> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativeStructuredReadOptions> native;
};

struct ScanOptions::Impl {
  explicit Impl(rust::Box<structured_ffi::NativeStructuredScanOptions> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativeStructuredScanOptions> native;
};

struct OwnedRow::Impl {
  explicit Impl(rust::Box<structured_ffi::NativeStructuredRow> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativeStructuredRow> native;
};

struct OwnedMultiGetResult::Impl {
  explicit Impl(rust::Box<structured_ffi::NativeStructuredMultiGetResult> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativeStructuredMultiGetResult> native;
};

struct OwnedBatch::Impl {
  explicit Impl(rust::Box<structured_ffi::NativeStructuredBatch> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativeStructuredBatch> native;
};

struct ScanCursor::Impl {
  explicit Impl(rust::Box<structured_ffi::NativeStructuredScanCursor> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativeStructuredScanCursor> native;
};

struct PendingShardSnapshot::Impl {
  explicit Impl(rust::Box<structured_ffi::NativePendingShardSnapshot> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativePendingShardSnapshot> native;
};

struct PendingSnapshot::Impl {
  explicit Impl(rust::Box<structured_ffi::NativePendingSnapshot> value)
      : native(std::move(value)) {}
  rust::Box<structured_ffi::NativePendingSnapshot> native;
};

struct SchemaBuilder::Impl {
  using Owner = std::variant<std::monostate, std::shared_ptr<Db::Impl>,
                             std::shared_ptr<SingleDb::Impl>>;

  Impl(Owner value, rust::Box<structured_ffi::NativeStructuredSchemaEdit> edit)
      : owner(std::move(value)), native(std::move(edit)) {}

  Owner owner;
  std::optional<rust::Box<structured_ffi::NativeStructuredSchemaEdit>> native;
  bool committed = false;
};

} // namespace cobble::structured
