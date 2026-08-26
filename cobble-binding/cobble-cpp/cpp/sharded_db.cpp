#include <cobble/db.hpp>

#include <utility>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"
#include "detail/options.hpp"

namespace cobble {
Db::Db(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
Db::Db(Db&&) noexcept = default;
Db& Db::operator=(Db&&) noexcept = default;
Db::~Db() = default;

Db Db::Open(std::string_view config_json) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_open(detail::RustStr(config_json));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::Open(std::string_view config_json,
            std::span<const BucketRange> ranges) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_open_ranges(
        detail::RustStr(config_json), detail::ToNativeRanges(ranges));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::OpenFile(std::string_view config_path) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_open_file(
        detail::RustStr(config_path));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::OpenFile(std::string_view config_path,
                std::span<const BucketRange> ranges) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_open_file_ranges(
        detail::RustStr(config_path), detail::ToNativeRanges(ranges));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::OpenFromSnapshot(std::string_view config_json, SnapshotId snapshot,
                        std::string_view existing_db_id, RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_open_from_snapshot(
        detail::RustStr(config_json), snapshot,
        detail::RustStr(existing_db_id), static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::OpenFromSnapshotFile(std::string_view config_path,
                            SnapshotId snapshot,
                            std::string_view existing_db_id,
                            RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_open_from_snapshot_file(
        detail::RustStr(config_path), snapshot,
        detail::RustStr(existing_db_id), static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::RestoreNew(std::string_view config_json, SnapshotId source_snapshot,
                  std::string_view source_db_id) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_restore_new(
        detail::RustStr(config_json), source_snapshot,
        detail::RustStr(source_db_id));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::RestoreNewFile(std::string_view config_path,
                      SnapshotId source_snapshot,
                      std::string_view source_db_id) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_restore_new_file(
        detail::RustStr(config_path), source_snapshot,
        detail::RustStr(source_db_id));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::RestoreNewFromManifest(std::string_view config_json,
                              std::string_view manifest_path) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_restore_new_from_manifest(
        detail::RustStr(config_json), detail::RustStr(manifest_path));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::RestoreNewFromManifestFile(std::string_view config_path,
                                  std::string_view manifest_path) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_restore_new_from_manifest_file(
        detail::RustStr(config_path), detail::RustStr(manifest_path));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::Resume(std::string_view config_json, std::string_view existing_db_id,
              RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_resume(
        detail::RustStr(config_json), detail::RustStr(existing_db_id),
        static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::ResumeFile(std::string_view config_path,
                  std::string_view existing_db_id, RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_resume_file(
        detail::RustStr(config_path), detail::RustStr(existing_db_id),
        static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::ResumeFromSnapshot(std::string_view config_json, SnapshotId snapshot,
                          std::string_view existing_db_id,
                          RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_resume_from_snapshot(
        detail::RustStr(config_json), snapshot,
        detail::RustStr(existing_db_id), static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

Db Db::ResumeFromSnapshotFile(std::string_view config_path,
                              SnapshotId snapshot,
                              std::string_view existing_db_id,
                              RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_resume_from_snapshot_file(
        detail::RustStr(config_path), snapshot,
        detail::RustStr(existing_db_id), static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_unique<Impl>(std::move(native)));
}

std::string Db::Id() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto id = ffi::native_sharded_database_id(*impl_->native);
  return {id.data(), id.size()};
}

void Db::Put(BucketId bucket, BytesView key, ColumnIndex column,
             BytesView value, const WriteOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    ffi::native_sharded_database_put(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::RustBytes(value), native_options);
  });
}

void Db::Delete(BucketId bucket, BytesView key, ColumnIndex column,
                const WriteOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    ffi::native_sharded_database_delete(*impl_->native, bucket,
                                         detail::RustBytes(key), column,
                                         native_options);
  });
}

void Db::Merge(BucketId bucket, BytesView key, ColumnIndex column,
               BytesView value, const WriteOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    ffi::native_sharded_database_merge(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::RustBytes(value), native_options);
  });
}

void Db::Write(WriteBatch batch, bool await_durable) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  if (!batch.impl_) {
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  }
  detail::Translate([&] {
    ffi::native_sharded_database_write_batch(
        *impl_->native, std::move(batch.impl_->native), await_durable);
  });
}

OwnedRow Db::Get(BucketId bucket, BytesView key,
                 const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_get(
        *impl_->native, bucket, detail::RustBytes(key), native_options);
  });
  return OwnedRow(std::make_unique<OwnedRow::Impl>(std::move(native)));
}

BufferResult Db::GetColumnInto(BucketId bucket, BytesView key,
                               MutableBytesView output,
                               const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  return detail::ToBufferResult(detail::Translate([&] {
    return ffi::native_sharded_database_get_column_into(
        *impl_->native, bucket, detail::RustBytes(key),
        detail::RustBytes(output), native_options);
  }));
}

ScanCursor Db::Scan(BucketId bucket,
                    std::optional<BytesView> start_inclusive,
                    std::optional<BytesView> end_exclusive,
                    const ScanOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  const auto start = start_inclusive.value_or(BytesView{});
  const auto end = end_exclusive.value_or(BytesView{});
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_scan(
        *impl_->native, bucket, detail::RustBytes(start),
        start_inclusive.has_value(), detail::RustBytes(end),
        end_exclusive.has_value(), native_options);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

void Db::SwitchToSnapshot(SnapshotId snapshot) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  detail::Translate([&] {
    ffi::native_sharded_database_switch_to_snapshot(*impl_->native, snapshot);
  });
}

void Db::Close() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  detail::Translate(
      [&] { ffi::native_sharded_database_close(*impl_->native); });
}

}  // namespace cobble
