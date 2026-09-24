#include <cobble/structured/read_only_db.hpp>

#include <utility>

#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {

ReadOnlyDb::ReadOnlyDb(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ReadOnlyDb::ReadOnlyDb(ReadOnlyDb &&) noexcept = default;
ReadOnlyDb &ReadOnlyDb::operator=(ReadOnlyDb &&) noexcept = default;
ReadOnlyDb::~ReadOnlyDb() = default;

ReadOnlyDb ReadOnlyDb::Open(std::string_view config_json, SnapshotId snapshot,
                            std::string_view source_db_id) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_read_only_db_open(
        detail::RustStr(config_json), snapshot, detail::RustStr(source_db_id));
  });
  return ReadOnlyDb(std::make_unique<Impl>(std::move(native)));
}

ReadOnlyDb ReadOnlyDb::OpenFile(std::string_view config_path,
                                SnapshotId snapshot,
                                std::string_view source_db_id) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_read_only_db_open_file(
        detail::RustStr(config_path), snapshot, detail::RustStr(source_db_id));
  });
  return ReadOnlyDb(std::make_unique<Impl>(std::move(native)));
}

std::string ReadOnlyDb::Id() const {
  const auto id =
      structured_ffi::native_structured_read_only_db_id(*impl_->native);
  return {id.data(), id.size()};
}

OwnedRow ReadOnlyDb::Get(BucketId bucket, BytesView key,
                         const ReadOptions &options) const {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_read_only_db_get(
        *impl_->native, bucket, detail::RustBytes(key), *options.impl_->native);
  });
  return OwnedRow(std::make_unique<OwnedRow::Impl>(std::move(native)));
}

OwnedRow ReadOnlyDb::Get(BucketId bucket, BytesView key) const {
  return Get(bucket, key, impl_->default_read_options);
}

ScanCursor ReadOnlyDb::Scan(BucketId bucket, BytesView start_inclusive,
                            BytesView end_exclusive,
                            const ScanOptions &options) const {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_read_only_db_scan(
        *impl_->native, bucket, detail::RustBytes(start_inclusive),
        detail::RustBytes(end_exclusive), *options.impl_->native);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

Schema ReadOnlyDb::CurrentSchema() const {
  return detail::ToSchema(
      structured_ffi::native_structured_read_only_db_current_schema(
          *impl_->native));
}

} // namespace cobble::structured
