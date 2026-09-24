#include <cobble/structured/reader.hpp>

#include <utility>

#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {

Reader::Reader(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
Reader::Reader(Reader &&) noexcept = default;
Reader &Reader::operator=(Reader &&) noexcept = default;
Reader::~Reader() = default;

Reader Reader::OpenCurrent(std::string_view config_json) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_reader_open_current(
        detail::RustStr(config_json));
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

Reader Reader::OpenCurrentFile(std::string_view config_path) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_reader_open_current_file(
        detail::RustStr(config_path));
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

Reader Reader::Open(std::string_view config_json, SnapshotId snapshot) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_reader_open(
        detail::RustStr(config_json), snapshot);
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

Reader Reader::OpenFile(std::string_view config_path, SnapshotId snapshot) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_reader_open_file(
        detail::RustStr(config_path), snapshot);
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

Reader Reader::Open(std::string_view config_json,
                    const GlobalSnapshot &snapshot) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_reader_open_from_global_snapshot(
        detail::RustStr(config_json), detail::ToNativeGlobalSnapshot(snapshot));
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

Reader Reader::OpenFile(std::string_view config_path,
                        const GlobalSnapshot &snapshot) {
  auto native = detail::Translate([&] {
    return structured_ffi::
        native_structured_reader_open_from_global_snapshot_file(
            detail::RustStr(config_path),
            detail::ToNativeGlobalSnapshot(snapshot));
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

void Reader::Refresh() {
  detail::Translate([&] {
    structured_ffi::native_structured_reader_refresh(*impl_->native);
  });
}

OwnedRow Reader::Get(BucketId bucket, BytesView key,
                     const ReadOptions &options) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_reader_get(
        *impl_->native, bucket, detail::RustBytes(key), *options.impl_->native);
  });
  return OwnedRow(std::make_unique<OwnedRow::Impl>(std::move(native)));
}

OwnedRow Reader::Get(BucketId bucket, BytesView key) {
  return Get(bucket, key, impl_->default_read_options);
}

ScanCursor Reader::Scan(BucketId bucket, BytesView start_inclusive,
                        BytesView end_exclusive, const ScanOptions &options) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_reader_scan(
        *impl_->native, bucket, detail::RustBytes(start_inclusive),
        detail::RustBytes(end_exclusive), *options.impl_->native);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

Schema Reader::CurrentSchema() const {
  return detail::ToSchema(
      structured_ffi::native_structured_reader_current_schema(*impl_->native));
}

ReaderMode Reader::Mode() const {
  return static_cast<ReaderMode>(
      structured_ffi::native_structured_reader_mode(*impl_->native));
}

std::optional<SnapshotId> Reader::ConfiguredSnapshotId() const {
  if (!structured_ffi::native_structured_reader_has_configured_snapshot(
          *impl_->native))
    return std::nullopt;
  return structured_ffi::native_structured_reader_configured_snapshot(
      *impl_->native);
}

GlobalSnapshot Reader::CurrentGlobalSnapshot() const {
  return detail::ToGlobalSnapshot(
      structured_ffi::native_structured_reader_current_global_snapshot(
          *impl_->native));
}

std::vector<GlobalSnapshot> Reader::ListGlobalSnapshots() const {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_reader_list_global_snapshots(
        *impl_->native);
  });
  std::vector<GlobalSnapshot> result;
  result.reserve(native.size());
  for (const auto &snapshot : native)
    result.push_back(detail::ToGlobalSnapshot(snapshot));
  return result;
}

}  // namespace cobble::structured
