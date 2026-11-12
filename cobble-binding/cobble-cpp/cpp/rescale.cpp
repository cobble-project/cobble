#include <cobble/db.hpp>

#include <limits>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"

namespace cobble {

SnapshotId Db::ExpandBucket(std::string_view source_db_id,
                            std::optional<SnapshotId> source_snapshot,
                            std::optional<std::span<const BucketRange>> ranges,
                            ImportStorageMode storage_mode,
                            RescaleOptions options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  auto native_ranges =
      ranges ? detail::ToNativeRanges(*ranges) : rust::Vec<ffi::NativeRange>();
  return detail::Translate([&] {
    return ffi::native_sharded_database_expand_bucket(
        *impl_->native, detail::RustStr(source_db_id),
        source_snapshot.has_value(), source_snapshot.value_or(0),
        ranges.has_value(), std::move(native_ranges),
        static_cast<std::uint8_t>(storage_mode), options.snapshot_before,
        options.snapshot_after);
  });
}

SnapshotId Db::ExpandBucketFromManifest(
    std::string_view source_db_id, std::string_view source_manifest_path,
    std::optional<std::span<const BucketRange>> ranges,
    ImportStorageMode storage_mode, RescaleOptions options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  auto native_ranges =
      ranges ? detail::ToNativeRanges(*ranges) : rust::Vec<ffi::NativeRange>();
  return detail::Translate([&] {
    return ffi::native_sharded_database_expand_bucket_from_manifest(
        *impl_->native, detail::RustStr(source_db_id),
        detail::RustStr(source_manifest_path), ranges.has_value(),
        std::move(native_ranges), static_cast<std::uint8_t>(storage_mode),
        options.snapshot_before, options.snapshot_after);
  });
}

void Db::WaitForImportAdoption(std::chrono::milliseconds timeout) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto count = timeout.count();
  if (count > std::numeric_limits<std::int64_t>::max() ||
      count < std::numeric_limits<std::int64_t>::min()) {
    throw Error(ErrorCode::kInput,
                "import adoption timeout does not fit in int64 milliseconds");
  }
  detail::Translate([&] {
    ffi::native_sharded_database_wait_for_import_adoption(
        *impl_->native, static_cast<std::int64_t>(count));
  });
}

SnapshotId Db::ShrinkBucket(std::span<const BucketRange> ranges) const {
  return ShrinkBucket(ranges, {}).value();
}

std::optional<SnapshotId> Db::ShrinkBucket(std::span<const BucketRange> ranges,
                                           RescaleOptions options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto snapshot = detail::Translate([&] {
    return ffi::native_sharded_database_shrink_bucket(
        *impl_->native, detail::ToNativeRanges(ranges), options.snapshot_before,
        options.snapshot_after);
  });
  if (!snapshot.has_snapshot) {
    return std::nullopt;
  }
  return snapshot.snapshot_id;
}

}  // namespace cobble
