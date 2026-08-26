#include <cobble/db.hpp>

#include <limits>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"

namespace cobble {

SnapshotId Db::ExpandBucket(
    std::string_view source_db_id,
    std::optional<SnapshotId> source_snapshot,
    std::optional<std::span<const BucketRange>> ranges,
    ExpandStorageMode storage_mode) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  auto native_ranges =
      ranges ? detail::ToNativeRanges(*ranges)
             : rust::Vec<ffi::NativeRange>();
  return detail::Translate([&] {
    return ffi::native_sharded_database_expand_bucket(
        *impl_->native, detail::RustStr(source_db_id),
        source_snapshot.has_value(), source_snapshot.value_or(0),
        ranges.has_value(), std::move(native_ranges),
        static_cast<std::uint8_t>(storage_mode));
  });
}

void Db::WaitForExpandAdoption(std::chrono::milliseconds timeout) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  const auto count = timeout.count();
  if (count > std::numeric_limits<std::int64_t>::max() ||
      count < std::numeric_limits<std::int64_t>::min()) {
    throw Error(ErrorCode::kInput,
                "expand adoption timeout does not fit in int64 milliseconds");
  }
  detail::Translate([&] {
    ffi::native_sharded_database_wait_for_expand_adoption(
        *impl_->native, static_cast<std::int64_t>(count));
  });
}

SnapshotId Db::ShrinkBucket(std::span<const BucketRange> ranges) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  return detail::Translate([&] {
    return ffi::native_sharded_database_shrink_bucket(
        *impl_->native, detail::ToNativeRanges(ranges));
  });
}

}  // namespace cobble
