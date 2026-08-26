#include <cobble/coordinator.hpp>

#include <utility>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"

namespace cobble {

DbCoordinator::DbCoordinator(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DbCoordinator::DbCoordinator(DbCoordinator&&) noexcept = default;
DbCoordinator& DbCoordinator::operator=(DbCoordinator&&) noexcept = default;
DbCoordinator::~DbCoordinator() = default;

DbCoordinator DbCoordinator::Open(std::string_view config_json) {
  auto native = detail::Translate([&] {
    return ffi::native_coordinator_open(detail::RustStr(config_json));
  });
  return DbCoordinator(std::make_unique<Impl>(std::move(native)));
}

DbCoordinator DbCoordinator::OpenFile(std::string_view config_path) {
  auto native = detail::Translate([&] {
    return ffi::native_coordinator_open_file(detail::RustStr(config_path));
  });
  return DbCoordinator(std::make_unique<Impl>(std::move(native)));
}

GlobalSnapshot DbCoordinator::MaterializeGlobalSnapshot(
    std::uint32_t total_buckets, SnapshotId snapshot,
    std::span<const ShardSnapshot> shards) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "DbCoordinator has been moved from");
  }
  return detail::ToGlobalSnapshot(detail::Translate([&] {
    return ffi::native_coordinator_materialize_global_snapshot(
        *impl_->native, total_buckets, snapshot,
        detail::ToNativeShardSnapshots(shards));
  }));
}

GlobalSnapshot DbCoordinator::GetGlobalSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "DbCoordinator has been moved from");
  }
  return detail::ToGlobalSnapshot(detail::Translate([&] {
    return ffi::native_coordinator_get_global_snapshot(*impl_->native,
                                                        snapshot);
  }));
}

std::vector<GlobalSnapshot> DbCoordinator::ListGlobalSnapshots() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "DbCoordinator has been moved from");
  }
  auto native = detail::Translate([&] {
    return ffi::native_coordinator_list_global_snapshots(*impl_->native);
  });
  std::vector<GlobalSnapshot> result;
  result.reserve(native.size());
  for (const auto& snapshot : native) {
    result.push_back(detail::ToGlobalSnapshot(snapshot));
  }
  return result;
}

std::optional<GlobalSnapshot> DbCoordinator::LoadCurrentGlobalSnapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "DbCoordinator has been moved from");
  }
  auto native = detail::Translate([&] {
    return ffi::native_coordinator_load_current_global_snapshot(*impl_->native);
  });
  if (native.empty()) {
    return std::nullopt;
  }
  if (native.size() != 1) {
    throw Error(ErrorCode::kInvalidState,
                "Rust bridge returned multiple current global snapshots");
  }
  return detail::ToGlobalSnapshot(native.front());
}

bool DbCoordinator::RetainSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "DbCoordinator has been moved from");
  }
  return ffi::native_coordinator_retain_snapshot(*impl_->native, snapshot);
}

bool DbCoordinator::ExpireSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "DbCoordinator has been moved from");
  }
  return detail::Translate([&] {
    return ffi::native_coordinator_expire_snapshot(*impl_->native, snapshot);
  });
}

}  // namespace cobble
