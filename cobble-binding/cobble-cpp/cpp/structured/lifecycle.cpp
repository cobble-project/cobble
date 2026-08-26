#include <cobble/structured/lifecycle.hpp>

#include <utility>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {

PendingShardSnapshot::PendingShardSnapshot(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
PendingShardSnapshot::PendingShardSnapshot(PendingShardSnapshot &&) noexcept =
    default;
PendingShardSnapshot &
PendingShardSnapshot::operator=(PendingShardSnapshot &&) noexcept = default;
PendingShardSnapshot::~PendingShardSnapshot() = default;

SnapshotId PendingShardSnapshot::id() const noexcept {
  return impl_
             ? structured_ffi::native_pending_shard_snapshot_id(*impl_->native)
             : 0;
}

ShardSnapshot PendingShardSnapshot::Wait() {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "PendingShardSnapshot has been moved from");
  }
  auto impl = std::move(impl_);
  return detail::ToShardSnapshot(detail::Translate([&] {
    return structured_ffi::native_pending_shard_snapshot_wait(*impl->native);
  }));
}

PendingSnapshot::PendingSnapshot(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
PendingSnapshot::PendingSnapshot(PendingSnapshot &&) noexcept = default;
PendingSnapshot &
PendingSnapshot::operator=(PendingSnapshot &&) noexcept = default;
PendingSnapshot::~PendingSnapshot() = default;

SnapshotId PendingSnapshot::id() const noexcept {
  return impl_ ? structured_ffi::native_pending_snapshot_id(*impl_->native) : 0;
}

GlobalSnapshot PendingSnapshot::Wait() {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "PendingSnapshot has been moved from");
  }
  auto impl = std::move(impl_);
  return detail::ToGlobalSnapshot(detail::Translate([&] {
    return structured_ffi::native_pending_snapshot_wait(*impl->native);
  }));
}

} // namespace cobble::structured
