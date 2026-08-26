#include <cobble/structured/db.hpp>

#include <limits>
#include <utility>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {

Db::Db(std::shared_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
Db::Db(Db &&) noexcept = default;
Db &Db::operator=(Db &&) noexcept = default;
Db::~Db() = default;

Db Db::Open(std::string_view config_json) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_open(
        detail::RustStr(config_json));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::Open(std::string_view config_json, std::span<const BucketRange> ranges) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_open_ranges(
        detail::RustStr(config_json), detail::ToNativeRanges(ranges));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::OpenFile(std::string_view config_path) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_open_file(
        detail::RustStr(config_path));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::OpenFile(std::string_view config_path,
                std::span<const BucketRange> ranges) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_open_file_ranges(
        detail::RustStr(config_path), detail::ToNativeRanges(ranges));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::OpenFromSnapshot(std::string_view config_json, SnapshotId snapshot,
                        std::string_view existing_db_id, RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_open_from_snapshot(
        detail::RustStr(config_json), snapshot, detail::RustStr(existing_db_id),
        static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::OpenFromSnapshotFile(std::string_view config_path, SnapshotId snapshot,
                            std::string_view existing_db_id,
                            RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_open_from_snapshot_file(
        detail::RustStr(config_path), snapshot, detail::RustStr(existing_db_id),
        static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::RestoreNew(std::string_view config_json, SnapshotId source_snapshot,
                  std::string_view source_db_id) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_restore_new(
        detail::RustStr(config_json), source_snapshot,
        detail::RustStr(source_db_id));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::RestoreNewFile(std::string_view config_path, SnapshotId source_snapshot,
                      std::string_view source_db_id) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_restore_new_file(
        detail::RustStr(config_path), source_snapshot,
        detail::RustStr(source_db_id));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::RestoreNewFromManifest(std::string_view config_json,
                              std::string_view manifest_path) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_restore_new_from_manifest(
        detail::RustStr(config_json), detail::RustStr(manifest_path));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::RestoreNewFromManifestFile(std::string_view config_path,
                                  std::string_view manifest_path) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_restore_new_from_manifest_file(
        detail::RustStr(config_path), detail::RustStr(manifest_path));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::Resume(std::string_view config_json, std::string_view existing_db_id,
              RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_resume(
        detail::RustStr(config_json), detail::RustStr(existing_db_id),
        static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::ResumeFile(std::string_view config_path, std::string_view existing_db_id,
                  RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_resume_file(
        detail::RustStr(config_path), detail::RustStr(existing_db_id),
        static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::ResumeFromSnapshot(std::string_view config_json, SnapshotId snapshot,
                          std::string_view existing_db_id, RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_resume_from_snapshot(
        detail::RustStr(config_json), snapshot, detail::RustStr(existing_db_id),
        static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

Db Db::ResumeFromSnapshotFile(std::string_view config_path, SnapshotId snapshot,
                              std::string_view existing_db_id,
                              RecoveryMode mode) {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_resume_from_snapshot_file(
        detail::RustStr(config_path), snapshot, detail::RustStr(existing_db_id),
        static_cast<std::uint8_t>(mode));
  });
  return Db(std::make_shared<Impl>(std::move(native)));
}

std::string Db::Id() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  }
  const auto id = structured_ffi::native_structured_db_id(*impl_->native);
  return {id.data(), id.size()};
}

void Db::PutBytes(BucketId bucket, BytesView key, ColumnIndex column,
                  BytesView value, const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_db_put_bytes(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::RustBytes(value), native_options);
  });
}
void Db::PutList(BucketId bucket, BytesView key, ColumnIndex column,
                 std::span<const BytesView> elements,
                 const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_db_put_list(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::ToNativeElements(elements), native_options);
  });
}
void Db::MergeBytes(BucketId bucket, BytesView key, ColumnIndex column,
                    BytesView value,
                    const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_db_merge_bytes(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::RustBytes(value), native_options);
  });
}
void Db::MergeList(BucketId bucket, BytesView key, ColumnIndex column,
                   std::span<const BytesView> elements,
                   const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_db_merge_list(
        *impl_->native, bucket, detail::RustBytes(key), column,
        detail::ToNativeElements(elements), native_options);
  });
}
void Db::Delete(BucketId bucket, BytesView key, ColumnIndex column,
                const cobble::WriteOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  const auto native_options = detail::ToNative(options);
  detail::Translate([&] {
    structured_ffi::native_structured_db_delete(
        *impl_->native, bucket, detail::RustBytes(key), column, native_options);
  });
}

OwnedRow Db::Get(BucketId bucket, BytesView key,
                 const ReadOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_get(
        *impl_->native, bucket, detail::RustBytes(key), *options.impl_->native);
  });
  return OwnedRow(std::make_unique<OwnedRow::Impl>(std::move(native)));
}

OwnedRow Db::Get(BucketId bucket, BytesView key) const {
  const ReadOptions options;
  return Get(bucket, key, options);
}

Schema Db::CurrentSchema() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return detail::ToSchema(
      structured_ffi::native_structured_db_current_schema(*impl_->native));
}

SchemaBuilder Db::UpdateSchema() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return SchemaBuilder(std::make_unique<SchemaBuilder::Impl>(
      SchemaBuilder::Impl::Owner(impl_),
      structured_ffi::native_structured_schema_edit_new()));
}

void Db::SetTime(std::uint32_t unix_seconds) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  structured_ffi::native_structured_db_set_time(*impl_->native, unix_seconds);
}
std::uint32_t Db::NowSeconds() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return structured_ffi::native_structured_db_now_seconds(*impl_->native);
}
SnapshotId Db::Snapshot() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_db_snapshot(*impl_->native);
  });
}

PendingShardSnapshot Db::StartSnapshot() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_start_snapshot(*impl_->native);
  });
  return PendingShardSnapshot(
      std::make_unique<PendingShardSnapshot::Impl>(std::move(native)));
}

ShardSnapshot Db::TakeSnapshot() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return detail::ToShardSnapshot(detail::Translate([&] {
    return structured_ffi::native_structured_db_take_snapshot(*impl_->native);
  }));
}

bool Db::CancelSnapshot(SnapshotId snapshot) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_db_cancel_snapshot(*impl_->native,
                                                                snapshot);
  });
}

ShardSnapshot Db::GetShardSnapshot(SnapshotId snapshot) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return detail::ToShardSnapshot(detail::Translate([&] {
    return structured_ffi::native_structured_db_get_shard_snapshot(
        *impl_->native, snapshot);
  }));
}

bool Db::RetainSnapshot(SnapshotId snapshot) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return structured_ffi::native_structured_db_retain_snapshot(*impl_->native,
                                                              snapshot);
}

bool Db::ExpireSnapshot(SnapshotId snapshot) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_db_expire_snapshot(*impl_->native,
                                                                snapshot);
  });
}

void Db::SwitchToSnapshot(SnapshotId snapshot) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  if (impl_.use_count() != 1) {
    throw Error(ErrorCode::kInvalidState,
                "SwitchToSnapshot requires releasing every scan cursor, "
                "schema builder, and priority queue first");
  }
  detail::Translate([&] {
    structured_ffi::native_structured_db_switch_to_snapshot(*impl_->native,
                                                            snapshot);
  });
}

std::vector<MetricSample> Db::Metrics() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return detail::ToMetrics(
      structured_ffi::native_structured_db_metrics(*impl_->native));
}

void Db::SwitchMemtableType(MemtableType type, bool flush_current) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  detail::Translate([&] {
    structured_ffi::native_structured_db_switch_memtable_type(
        *impl_->native, static_cast<std::uint8_t>(type), flush_current);
  });
}

std::size_t Db::LoadReadonlyFilesToPrimary() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  const auto value = detail::Translate([&] {
    return structured_ffi::native_structured_db_load_readonly_files_to_primary(
        *impl_->native);
  });
  if (value > std::numeric_limits<std::size_t>::max()) {
    throw Error(ErrorCode::kInvalidState, "readonly file count exceeds size_t");
  }
  return static_cast<std::size_t>(value);
}

SnapshotId Db::ExpandBucket(std::string_view source_db_id,
                            std::optional<SnapshotId> source_snapshot,
                            std::optional<std::span<const BucketRange>> ranges,
                            ExpandStorageMode storage_mode) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_db_expand_bucket(
        *impl_->native, detail::RustStr(source_db_id),
        source_snapshot.has_value(), source_snapshot.value_or(0),
        ranges.has_value(),
        ranges ? detail::ToNativeRanges(*ranges)
               : rust::Vec<structured_ffi::NativeBucketRange>{},
        static_cast<std::uint8_t>(storage_mode));
  });
}

void Db::WaitForExpandAdoption(std::chrono::milliseconds timeout) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  detail::Translate([&] {
    structured_ffi::native_structured_db_wait_for_expand_adoption(
        *impl_->native, timeout.count());
  });
}

SnapshotId Db::ShrinkBucket(std::span<const BucketRange> ranges) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_db_shrink_bucket(
        *impl_->native, detail::ToNativeRanges(ranges));
  });
}

void Db::Close() const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  detail::Translate(
      [&] { structured_ffi::native_structured_db_close(*impl_->native); });
}

} // namespace cobble::structured
