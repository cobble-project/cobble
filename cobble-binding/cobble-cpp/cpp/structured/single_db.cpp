#include <cobble/structured/single_db.hpp>

#include <limits>
#include <utility>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {

SingleDb::SingleDb(std::shared_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SingleDb::SingleDb(SingleDb &&) noexcept = default;
SingleDb &SingleDb::operator=(SingleDb &&) noexcept = default;
SingleDb::~SingleDb() = default;

SingleDb SingleDb::Open(std::string_view config_json) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_open(
        detail::RustStr(config_json));
  });
  return SingleDb(std::make_shared<Impl>(std::move(native)));
}
SingleDb SingleDb::OpenFile(std::string_view config_path) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_open_file(
        detail::RustStr(config_path));
  });
  return SingleDb(std::make_shared<Impl>(std::move(native)));
}
void SingleDb::PutBytes(BucketId bucket, BytesView key, ColumnIndex column,
                        BytesView value,
                        const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_single_db_put_bytes(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::RustBytes(value), native_options);
  });
}
void SingleDb::PutList(BucketId bucket, BytesView key, ColumnIndex column,
                       std::span<const BytesView> elements,
                       const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_single_db_put_list(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::ToNativeElements(elements), native_options);
  });
}
void SingleDb::MergeBytes(BucketId bucket, BytesView key, ColumnIndex column,
                          BytesView value,
                          const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_single_db_merge_bytes(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::RustBytes(value), native_options);
  });
}
void SingleDb::MergeList(BucketId bucket, BytesView key, ColumnIndex column,
                         std::span<const BytesView> elements,
                         const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_single_db_merge_list(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::ToNativeElements(elements), native_options);
  });
}
void SingleDb::Delete(BucketId bucket, BytesView key, ColumnIndex column,
                      const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_single_db_delete(
        *impl_->native, bucket, detail::RustBytes(key), column, native_options);
  });
}
OwnedRow SingleDb::Get(BucketId bucket, BytesView key,
                       const ReadOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_get(
        *impl_->native, bucket, detail::RustBytes(key), *options.impl_->native);
  });
  return OwnedRow(std::make_unique<OwnedRow::Impl>(std::move(native)));
}
OwnedRow SingleDb::Get(BucketId bucket, BytesView key) const {
  const ReadOptions options;
  return Get(bucket, key, options);
}
Schema SingleDb::CurrentSchema() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  return detail::ToSchema(
      structured_ffi::native_structured_single_db_current_schema(
          *impl_->native));
}
SchemaBuilder SingleDb::UpdateSchema() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  return SchemaBuilder(std::make_unique<SchemaBuilder::Impl>(
      SchemaBuilder::Impl::Owner(impl_),
      structured_ffi::native_structured_schema_edit_new()));
}
void SingleDb::SetTime(std::uint32_t unix_seconds) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  structured_ffi::native_structured_single_db_set_time(*impl_->native,
                                                       unix_seconds);
}
std::uint32_t SingleDb::NowSeconds() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  return structured_ffi::native_structured_single_db_now_seconds(
      *impl_->native);
}
SnapshotId SingleDb::Snapshot() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_single_db_snapshot(*impl_->native);
  });
}

PendingSnapshot SingleDb::StartSnapshot() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_start_snapshot(
        *impl_->native);
  });
  return PendingSnapshot(
      std::make_unique<PendingSnapshot::Impl>(std::move(native)));
}

GlobalSnapshot SingleDb::TakeSnapshot() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  return detail::ToGlobalSnapshot(detail::Translate([&] {
    return structured_ffi::native_structured_single_db_take_snapshot(
        *impl_->native);
  }));
}

std::vector<GlobalSnapshot> SingleDb::ListSnapshots() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_list_snapshots(
        *impl_->native);
  });
  std::vector<GlobalSnapshot> result;
  result.reserve(native.size());
  for (const auto &snapshot : native) {
    result.push_back(detail::ToGlobalSnapshot(snapshot));
  }
  return result;
}

bool SingleDb::RetainSnapshot(SnapshotId snapshot) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_single_db_retain_snapshot(
        *impl_->native, snapshot);
  });
}

bool SingleDb::ExpireSnapshot(SnapshotId snapshot) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_single_db_expire_snapshot(
        *impl_->native, snapshot);
  });
}

void SingleDb::SwitchMemtableType(MemtableType type, bool flush_current) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  detail::Translate([&] {
    structured_ffi::native_structured_single_db_switch_memtable_type(
        *impl_->native, static_cast<std::uint8_t>(type), flush_current);
  });
}

std::size_t SingleDb::LoadReadonlyFilesToPrimary() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  const auto value = detail::Translate([&] {
    return structured_ffi::
        native_structured_single_db_load_readonly_files_to_primary(
            *impl_->native);
  });
  if (value > std::numeric_limits<std::size_t>::max()) {
    throw Error(ErrorCode::kInvalidState, "readonly file count exceeds size_t");
  }
  return static_cast<std::size_t>(value);
}

void SingleDb::Close() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  detail::Translate([&] {
    structured_ffi::native_structured_single_db_close(*impl_->native);
  });
}

} // namespace cobble::structured
