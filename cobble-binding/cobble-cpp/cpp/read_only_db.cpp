#include <cobble/read_only_db.hpp>

#include <utility>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"
#include "detail/options.hpp"

namespace cobble {

ReadOnlyDb::ReadOnlyDb(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ReadOnlyDb::ReadOnlyDb(ReadOnlyDb&&) noexcept = default;
ReadOnlyDb& ReadOnlyDb::operator=(ReadOnlyDb&&) noexcept = default;
ReadOnlyDb::~ReadOnlyDb() = default;

ReadOnlyDb ReadOnlyDb::Open(std::string_view config_json, SnapshotId snapshot,
                            std::string_view source_db_id) {
  auto native = detail::Translate([&] {
    return ffi::native_read_only_database_open(
        detail::RustStr(config_json), snapshot,
        detail::RustStr(source_db_id));
  });
  return ReadOnlyDb(std::make_unique<Impl>(std::move(native)));
}

ReadOnlyDb ReadOnlyDb::OpenFile(std::string_view config_path,
                                SnapshotId snapshot,
                                std::string_view source_db_id) {
  auto native = detail::Translate([&] {
    return ffi::native_read_only_database_open_file(
        detail::RustStr(config_path), snapshot,
        detail::RustStr(source_db_id));
  });
  return ReadOnlyDb(std::make_unique<Impl>(std::move(native)));
}

std::string ReadOnlyDb::Id() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ReadOnlyDb has been moved from");
  }
  const auto id = ffi::native_read_only_database_id(*impl_->native);
  return {id.data(), id.size()};
}

OwnedRow ReadOnlyDb::Get(BucketId bucket, BytesView key,
                         const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ReadOnlyDb has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  auto native = detail::Translate([&] {
    return ffi::native_read_only_database_get(
        *impl_->native, bucket, detail::RustBytes(key), native_options);
  });
  return OwnedRow(std::make_unique<OwnedRow::Impl>(std::move(native)));
}

BufferResult ReadOnlyDb::GetColumnInto(BucketId bucket, BytesView key,
                                       MutableBytesView output,
                                       const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ReadOnlyDb has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  return detail::ToBufferResult(detail::Translate([&] {
    return ffi::native_read_only_database_get_column_into(
        *impl_->native, bucket, detail::RustBytes(key),
        detail::RustBytes(output), native_options);
  }));
}

ScanCursor ReadOnlyDb::Scan(BucketId bucket,
                            std::optional<BytesView> start_inclusive,
                            std::optional<BytesView> end_exclusive,
                            const ScanOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ReadOnlyDb has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  const auto start = start_inclusive.value_or(BytesView{});
  const auto end = end_exclusive.value_or(BytesView{});
  auto native = detail::Translate([&] {
    return ffi::native_read_only_database_scan(
        *impl_->native, bucket, detail::RustBytes(start),
        start_inclusive.has_value(), detail::RustBytes(end),
        end_exclusive.has_value(), native_options);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

Schema ReadOnlyDb::CurrentSchema() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ReadOnlyDb has been moved from");
  }
  return detail::ToSchema(detail::Translate([&] {
    return ffi::native_read_only_database_current_schema(*impl_->native);
  }));
}

std::vector<MetricSample> ReadOnlyDb::Metrics() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ReadOnlyDb has been moved from");
  }
  return detail::ToMetrics(
      ffi::native_read_only_database_metrics(*impl_->native));
}

}  // namespace cobble
