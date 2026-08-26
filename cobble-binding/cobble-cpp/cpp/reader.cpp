#include <cobble/reader.hpp>

#include <utility>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"
#include "detail/options.hpp"

namespace cobble {

Reader::Reader(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
Reader::Reader(Reader&&) noexcept = default;
Reader& Reader::operator=(Reader&&) noexcept = default;
Reader::~Reader() = default;

Reader Reader::OpenCurrent(std::string_view config_json) {
  auto native = detail::Translate([&] {
    return ffi::native_reader_open_current(detail::RustStr(config_json));
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

Reader Reader::OpenCurrentFile(std::string_view config_path) {
  auto native = detail::Translate([&] {
    return ffi::native_reader_open_current_file(detail::RustStr(config_path));
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

Reader Reader::Open(std::string_view config_json, SnapshotId snapshot) {
  auto native = detail::Translate([&] {
    return ffi::native_reader_open(detail::RustStr(config_json), snapshot);
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

Reader Reader::OpenFile(std::string_view config_path, SnapshotId snapshot) {
  auto native = detail::Translate([&] {
    return ffi::native_reader_open_file(detail::RustStr(config_path), snapshot);
  });
  return Reader(std::make_unique<Impl>(std::move(native)));
}

void Reader::Refresh() {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Reader has been moved from");
  }
  detail::Translate([&] { ffi::native_reader_refresh(*impl_->native); });
}

OwnedRow Reader::Get(BucketId bucket, BytesView key,
                     const ReadOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Reader has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  auto native = detail::Translate([&] {
    return ffi::native_reader_get(*impl_->native, bucket,
                                  detail::RustBytes(key), native_options);
  });
  return OwnedRow(std::make_unique<OwnedRow::Impl>(std::move(native)));
}

BufferResult Reader::GetColumnInto(BucketId bucket, BytesView key,
                                   MutableBytesView output,
                                   const ReadOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Reader has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  return detail::ToBufferResult(detail::Translate([&] {
    return ffi::native_reader_get_column_into(
        *impl_->native, bucket, detail::RustBytes(key),
        detail::RustBytes(output), native_options);
  }));
}

ScanCursor Reader::Scan(BucketId bucket, BytesView start_inclusive,
                        BytesView end_exclusive,
                        const ScanOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Reader has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  auto native = detail::Translate([&] {
    return ffi::native_reader_scan(
        *impl_->native, bucket, detail::RustBytes(start_inclusive),
        detail::RustBytes(end_exclusive), native_options);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

ReaderMode Reader::Mode() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Reader has been moved from");
  }
  switch (ffi::native_reader_mode(*impl_->native)) {
    case 0:
      return ReaderMode::kCurrent;
    case 1:
      return ReaderMode::kSnapshot;
    default:
      throw Error(ErrorCode::kInvalidState,
                  "Rust bridge returned an unknown Reader mode");
  }
}

std::optional<SnapshotId> Reader::ConfiguredSnapshotId() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Reader has been moved from");
  }
  if (!ffi::native_reader_has_configured_snapshot(*impl_->native)) {
    return std::nullopt;
  }
  return ffi::native_reader_configured_snapshot(*impl_->native);
}

GlobalSnapshot Reader::CurrentGlobalSnapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Reader has been moved from");
  }
  return detail::ToGlobalSnapshot(
      ffi::native_reader_current_global_snapshot(*impl_->native));
}

std::vector<GlobalSnapshot> Reader::ListGlobalSnapshots() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Reader has been moved from");
  }
  auto native = detail::Translate(
      [&] { return ffi::native_reader_list_global_snapshots(*impl_->native); });
  std::vector<GlobalSnapshot> result;
  result.reserve(native.size());
  for (const auto& snapshot : native) {
    result.push_back(detail::ToGlobalSnapshot(snapshot));
  }
  return result;
}

}  // namespace cobble
