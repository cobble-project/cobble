#include <cobble/database.hpp>

#include <limits>
#include <utility>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"
#include "detail/options.hpp"

namespace cobble {

OwnedRow::OwnedRow(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedRow::OwnedRow(OwnedRow&&) noexcept = default;
OwnedRow& OwnedRow::operator=(OwnedRow&&) noexcept = default;
OwnedRow::~OwnedRow() = default;

bool OwnedRow::found() const noexcept {
  return impl_ && ffi::native_row_found(*impl_->native);
}

std::size_t OwnedRow::column_count() const noexcept {
  if (!impl_) {
    return 0;
  }
  const auto count = ffi::native_row_column_count(*impl_->native);
  return count > std::numeric_limits<std::size_t>::max()
             ? std::numeric_limits<std::size_t>::max()
             : static_cast<std::size_t>(count);
}

bool OwnedRow::has_column(std::size_t column) const {
  return impl_ && ffi::native_row_has_column(*impl_->native, column);
}

BytesView OwnedRow::column(std::size_t column) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedRow has been moved from");
  }
  return detail::Translate([&] {
    return detail::ToView(ffi::native_row_column(*impl_->native, column));
  });
}

Database::Database(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
Database::Database(Database&&) noexcept = default;
Database& Database::operator=(Database&&) noexcept = default;
Database::~Database() = default;

Database Database::Open(std::string_view config_json) {
  auto native = detail::Translate(
      [&] { return ffi::native_database_open(detail::RustStr(config_json)); });
  return Database(std::make_unique<Impl>(std::move(native)));
}

Database Database::OpenFile(std::string_view config_path) {
  auto native = detail::Translate(
      [&] { return ffi::native_database_open_file(detail::RustStr(config_path)); });
  return Database(std::make_unique<Impl>(std::move(native)));
}

Database Database::Resume(std::string_view config_json, SnapshotId snapshot,
                          RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return ffi::native_database_resume(detail::RustStr(config_json), snapshot,
                                       static_cast<std::uint8_t>(mode));
  });
  return Database(std::make_unique<Impl>(std::move(native)));
}

Database Database::ResumeFile(std::string_view config_path, SnapshotId snapshot,
                              RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return ffi::native_database_resume_file(detail::RustStr(config_path), snapshot,
                                            static_cast<std::uint8_t>(mode));
  });
  return Database(std::make_unique<Impl>(std::move(native)));
}

void Database::Put(BucketId bucket, BytesView key, ColumnIndex column,
                   BytesView value, const WriteOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    ffi::native_database_put(*impl_->native, bucket, detail::RustBytes(key), column,
                             detail::RustBytes(value), native_options);
  });
}

void Database::Delete(BucketId bucket, BytesView key, ColumnIndex column,
                      const WriteOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    ffi::native_database_delete(*impl_->native, bucket, detail::RustBytes(key), column,
                                native_options);
  });
}

void Database::Merge(BucketId bucket, BytesView key, ColumnIndex column,
                     BytesView value, const WriteOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    ffi::native_database_merge(*impl_->native, bucket, detail::RustBytes(key), column,
                               detail::RustBytes(value), native_options);
  });
}

void Database::Write(WriteBatch batch, bool await_durable) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  if (!batch.impl_) {
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  }
  detail::Translate([&] {
    ffi::native_database_write_batch(*impl_->native, std::move(batch.impl_->native),
                                     await_durable);
  });
}

OwnedRow Database::Get(BucketId bucket, BytesView key,
                       const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  auto native = detail::Translate([&] {
    return ffi::native_database_get(*impl_->native, bucket, detail::RustBytes(key),
                                    native_options);
  });
  return OwnedRow(std::make_unique<OwnedRow::Impl>(std::move(native)));
}

BufferResult Database::GetColumnInto(BucketId bucket, BytesView key,
                                     MutableBytesView output,
                                     const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  return detail::ToBufferResult(detail::Translate([&] {
    return ffi::native_database_get_column_into(
        *impl_->native, bucket, detail::RustBytes(key),
        detail::RustBytes(output), native_options);
  }));
}

ScanCursor Database::Scan(BucketId bucket, std::optional<BytesView> start_inclusive,
                          std::optional<BytesView> end_exclusive,
                          const ScanOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  const BytesView start = start_inclusive.value_or(BytesView{});
  const BytesView end = end_exclusive.value_or(BytesView{});
  auto native = detail::Translate([&] {
    return ffi::native_database_scan(
        *impl_->native, bucket, detail::RustBytes(start),
        start_inclusive.has_value(), detail::RustBytes(end),
        end_exclusive.has_value(), native_options);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

SnapshotId Database::Snapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return detail::Translate(
      [&] { return ffi::native_database_snapshot(*impl_->native); });
}

bool Database::RetainSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return detail::Translate([&] {
    return ffi::native_database_retain_snapshot(*impl_->native, snapshot);
  });
}

bool Database::ExpireSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return detail::Translate([&] {
    return ffi::native_database_expire_snapshot(*impl_->native, snapshot);
  });
}

std::vector<SnapshotId> Database::ListSnapshots() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  auto native = detail::Translate(
      [&] { return ffi::native_database_list_snapshots(*impl_->native); });
  return {native.begin(), native.end()};
}

std::string Database::SnapshotManifestJson(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  auto native = detail::Translate([&] {
    return ffi::native_database_snapshot_manifest_json(*impl_->native, snapshot);
  });
  return {native.data(), native.size()};
}

void Database::SetTime(std::uint32_t unix_seconds) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  ffi::native_database_set_time(*impl_->native, unix_seconds);
}

void Database::Close() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  detail::Translate([&] { ffi::native_database_close(*impl_->native); });
}

}  // namespace cobble
