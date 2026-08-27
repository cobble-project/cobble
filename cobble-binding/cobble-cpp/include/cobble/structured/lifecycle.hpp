#pragma once

#include <memory>

#include <cobble/snapshot.hpp>
#include <cobble/structured/types.hpp>

namespace cobble::structured {

class COBBLE_CPP_API PendingShardSnapshot final {
public:
  PendingShardSnapshot(PendingShardSnapshot &&) noexcept;
  PendingShardSnapshot &operator=(PendingShardSnapshot &&) noexcept;
  ~PendingShardSnapshot();
  PendingShardSnapshot(const PendingShardSnapshot &) = delete;
  PendingShardSnapshot &operator=(const PendingShardSnapshot &) = delete;

  [[nodiscard]] SnapshotId id() const noexcept;
  [[nodiscard]] ShardSnapshot Wait();

private:
  struct Impl;
  explicit PendingShardSnapshot(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
  friend class Db;
};

class COBBLE_CPP_API PendingSnapshot final {
public:
  PendingSnapshot(PendingSnapshot &&) noexcept;
  PendingSnapshot &operator=(PendingSnapshot &&) noexcept;
  ~PendingSnapshot();
  PendingSnapshot(const PendingSnapshot &) = delete;
  PendingSnapshot &operator=(const PendingSnapshot &) = delete;

  [[nodiscard]] SnapshotId id() const noexcept;
  [[nodiscard]] GlobalSnapshot Wait();

private:
  struct Impl;
  explicit PendingSnapshot(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
  friend class SingleDb;
};

} // namespace cobble::structured
