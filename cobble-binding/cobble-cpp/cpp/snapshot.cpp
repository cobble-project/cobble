#include <cobble/database.hpp>

#include <utility>

#include "detail/error.hpp"
#include "detail/impl.hpp"

namespace cobble {
namespace detail {

ShardSnapshot ToShardSnapshot(const ffi::NativeShardSnapshot& native) {
  ShardSnapshot shard;
  shard.db_id = std::string(native.db_id);
  shard.snapshot_id = native.snapshot_id;
  shard.manifest_path = std::string(native.manifest_path);
  shard.timestamp_seconds = native.timestamp_seconds;
  shard.data_size_bytes = native.data_size_bytes;
  shard.incremental_data_size_bytes = native.incremental_data_size_bytes;
  shard.ranges.reserve(native.ranges.size());
  for (const auto& range : native.ranges) {
    shard.ranges.push_back({range.first, range.last});
  }
  shard.column_families.reserve(native.families.size());
  for (const auto& family : native.families) {
    shard.column_families.push_back({std::string(family.name), family.id});
  }
  return shard;
}

GlobalSnapshot ToGlobalSnapshot(const ffi::NativeSnapshot& native) {
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
    result.shards.push_back(detail::ToShardSnapshot(native_shard));
  }
  return result;
}

}  // namespace detail

struct PendingSnapshot::Impl {
  explicit Impl(rust::Box<ffi::NativePendingSnapshot> native_snapshot)
      : native(std::move(native_snapshot)) {}

  rust::Box<ffi::NativePendingSnapshot> native;
};

struct PendingShardSnapshot::Impl {
  explicit Impl(rust::Box<ffi::NativePendingShardSnapshot> native_snapshot)
      : native(std::move(native_snapshot)) {}

  rust::Box<ffi::NativePendingShardSnapshot> native;
};

PendingSnapshot::PendingSnapshot(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
PendingSnapshot::PendingSnapshot(PendingSnapshot&&) noexcept = default;
PendingSnapshot& PendingSnapshot::operator=(PendingSnapshot&&) noexcept =
    default;
PendingSnapshot::~PendingSnapshot() = default;

PendingShardSnapshot::PendingShardSnapshot(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
PendingShardSnapshot::PendingShardSnapshot(PendingShardSnapshot&&) noexcept =
    default;
PendingShardSnapshot& PendingShardSnapshot::operator=(
    PendingShardSnapshot&&) noexcept = default;
PendingShardSnapshot::~PendingShardSnapshot() = default;

SnapshotId PendingSnapshot::id() const noexcept {
  return impl_ ? ffi::native_pending_snapshot_id(*impl_->native) : 0;
}

GlobalSnapshot PendingSnapshot::Wait() {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "PendingSnapshot has been moved from");
  }
  auto impl = std::move(impl_);
  return detail::ToGlobalSnapshot(detail::Translate(
      [&] { return ffi::native_pending_snapshot_wait(*impl->native); }));
}

SnapshotId PendingShardSnapshot::id() const noexcept {
  return impl_ ? ffi::native_pending_shard_snapshot_id(*impl_->native) : 0;
}

ShardSnapshot PendingShardSnapshot::Wait() {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "PendingShardSnapshot has been moved from");
  }
  auto impl = std::move(impl_);
  return detail::ToShardSnapshot(detail::Translate([&] {
    return ffi::native_pending_shard_snapshot_wait(*impl->native);
  }));
}

GlobalSnapshot Database::TakeSnapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return detail::ToGlobalSnapshot(detail::Translate(
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
  return detail::ToGlobalSnapshot(detail::Translate([&] {
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
    result.push_back(detail::ToGlobalSnapshot(snapshot));
  }
  return result;
}

SnapshotId Db::Snapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  return detail::Translate(
      [&] { return ffi::native_sharded_database_snapshot(*impl_->native); });
}

PendingShardSnapshot Db::StartSnapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  auto native = detail::Translate([&] {
    return ffi::native_sharded_database_start_snapshot(*impl_->native);
  });
  return PendingShardSnapshot(
      std::make_unique<PendingShardSnapshot::Impl>(std::move(native)));
}

ShardSnapshot Db::TakeSnapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  return detail::ToShardSnapshot(detail::Translate([&] {
    return ffi::native_sharded_database_take_snapshot(*impl_->native);
  }));
}

bool Db::CancelSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  return detail::Translate([&] {
    return ffi::native_sharded_database_cancel_snapshot(*impl_->native,
                                                         snapshot);
  });
}

ShardSnapshot Db::GetShardSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  return detail::ToShardSnapshot(detail::Translate([&] {
    return ffi::native_sharded_database_get_shard_snapshot(*impl_->native,
                                                            snapshot);
  }));
}

bool Db::RetainSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  return ffi::native_sharded_database_retain_snapshot(*impl_->native,
                                                       snapshot);
}

bool Db::ExpireSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  return detail::Translate([&] {
    return ffi::native_sharded_database_expire_snapshot(*impl_->native,
                                                         snapshot);
  });
}

}  // namespace cobble
