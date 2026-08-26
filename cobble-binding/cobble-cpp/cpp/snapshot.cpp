#include <cobble/database.hpp>

#include <utility>

#include "detail/error.hpp"
#include "detail/impl.hpp"

namespace cobble {
namespace {

GlobalSnapshot ToSnapshot(const ffi::NativeSnapshot& native) {
  GlobalSnapshot result{
      native.version,
      native.id,
      native.total_buckets,
      {},
      {},
      native.watermark_seconds,
  };
  result.column_families.reserve(native.families.size());
  for (const auto& family : native.families) {
    result.column_families.push_back({std::string(family.name), family.id});
  }
  result.shards.reserve(native.shards.size());
  for (const auto& native_shard : native.shards) {
    ShardSnapshot shard;
    shard.db_id = std::string(native_shard.db_id);
    shard.snapshot_id = native_shard.snapshot_id;
    shard.manifest_path = std::string(native_shard.manifest_path);
    shard.timestamp_seconds = native_shard.timestamp_seconds;
    shard.data_size_bytes = native_shard.data_size_bytes;
    shard.incremental_data_size_bytes =
        native_shard.incremental_data_size_bytes;
    shard.ranges.reserve(native_shard.ranges.size());
    for (const auto& range : native_shard.ranges) {
      shard.ranges.push_back({range.first, range.last});
    }
    shard.column_families.reserve(native_shard.families.size());
    for (const auto& family : native_shard.families) {
      shard.column_families.push_back(
          {std::string(family.name), family.id});
    }
    result.shards.push_back(std::move(shard));
  }
  return result;
}

}  // namespace

struct PendingSnapshot::Impl {
  explicit Impl(rust::Box<ffi::NativePendingSnapshot> native_snapshot)
      : native(std::move(native_snapshot)) {}

  rust::Box<ffi::NativePendingSnapshot> native;
};

PendingSnapshot::PendingSnapshot(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
PendingSnapshot::PendingSnapshot(PendingSnapshot&&) noexcept = default;
PendingSnapshot& PendingSnapshot::operator=(PendingSnapshot&&) noexcept =
    default;
PendingSnapshot::~PendingSnapshot() = default;

SnapshotId PendingSnapshot::id() const noexcept {
  return impl_ ? ffi::native_pending_snapshot_id(*impl_->native) : 0;
}

GlobalSnapshot PendingSnapshot::Wait() {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "PendingSnapshot has been moved from");
  }
  auto impl = std::move(impl_);
  return ToSnapshot(detail::Translate(
      [&] { return ffi::native_pending_snapshot_wait(*impl->native); }));
}

GlobalSnapshot Database::TakeSnapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return ToSnapshot(detail::Translate(
      [&] { return ffi::native_database_take_snapshot(*impl_->native); }));
}

PendingSnapshot Database::StartSnapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  auto native = detail::Translate(
      [&] { return ffi::native_database_start_snapshot(*impl_->native); });
  return PendingSnapshot(
      std::make_unique<PendingSnapshot::Impl>(std::move(native)));
}

GlobalSnapshot Database::GetSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return ToSnapshot(detail::Translate([&] {
    return ffi::native_database_get_snapshot_typed(*impl_->native, snapshot);
  }));
}

std::vector<GlobalSnapshot> Database::ListGlobalSnapshots() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  auto native = detail::Translate(
      [&] { return ffi::native_database_list_snapshots_typed(*impl_->native); });
  std::vector<GlobalSnapshot> result;
  result.reserve(native.size());
  for (const auto& snapshot : native) {
    result.push_back(ToSnapshot(snapshot));
  }
  return result;
}

}  // namespace cobble
