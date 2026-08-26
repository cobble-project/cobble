#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <cobble/structured/types.hpp>

namespace cobble::structured {

class Db;
class SingleDb;

struct PriorityQueueEntryView {
  BytesView key;
  BytesView value;
};

// Move-only owner of one Rust Bytes-backed queue entry. Views remain valid
// until this object is destroyed or moved from.
class COBBLE_CPP_API OwnedPriorityQueueEntry final {
public:
  OwnedPriorityQueueEntry(OwnedPriorityQueueEntry &&) noexcept;
  OwnedPriorityQueueEntry &operator=(OwnedPriorityQueueEntry &&) noexcept;
  ~OwnedPriorityQueueEntry();

  OwnedPriorityQueueEntry(const OwnedPriorityQueueEntry &) = delete;
  OwnedPriorityQueueEntry &
  operator=(const OwnedPriorityQueueEntry &) = delete;

  [[nodiscard]] BytesView Key() const;
  [[nodiscard]] BytesView Value() const;

private:
  struct Impl;
  explicit OwnedPriorityQueueEntry(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class PriorityQueue;
};

// Move-only owner of a Rust Bytes-backed queue batch. Entry views remain valid
// until this object is destroyed or moved from.
class COBBLE_CPP_API OwnedPriorityQueueBatch final {
public:
  OwnedPriorityQueueBatch(OwnedPriorityQueueBatch &&) noexcept;
  OwnedPriorityQueueBatch &operator=(OwnedPriorityQueueBatch &&) noexcept;
  ~OwnedPriorityQueueBatch();

  OwnedPriorityQueueBatch(const OwnedPriorityQueueBatch &) = delete;
  OwnedPriorityQueueBatch &operator=(const OwnedPriorityQueueBatch &) = delete;

  [[nodiscard]] std::size_t Size() const noexcept;
  [[nodiscard]] PriorityQueueEntryView Entry(std::size_t index) const;

private:
  struct Impl;
  explicit OwnedPriorityQueueBatch(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class PriorityQueue;
};

// A detached queue descriptor with shared concrete database ownership.
//
// All operations are synchronous. Input views are borrowed only for the call;
// returned owners retain Rust allocations without payload copies. Instances
// must be externally synchronized when used concurrently. A live queue is a
// child owner: release it before schema commits or Db::SwitchToSnapshot.
class COBBLE_CPP_API PriorityQueue final {
public:
  PriorityQueue(PriorityQueue &&) noexcept;
  PriorityQueue &operator=(PriorityQueue &&) noexcept;
  ~PriorityQueue();

  PriorityQueue(const PriorityQueue &) = delete;
  PriorityQueue &operator=(const PriorityQueue &) = delete;

  [[nodiscard]] std::string ColumnFamily() const;
  void Offer(BucketId bucket, BytesView key, BytesView value) const;
  void Delete(BucketId bucket, BytesView key) const;

  [[nodiscard]] std::optional<OwnedPriorityQueueEntry>
  Peek(BucketId bucket) const;
  [[nodiscard]] std::optional<OwnedPriorityQueueEntry>
  Poll(BucketId bucket) const;
  [[nodiscard]] OwnedPriorityQueueBatch
  PeekBatch(BucketId bucket,
            std::optional<std::size_t> limit = std::nullopt) const;
  [[nodiscard]] OwnedPriorityQueueBatch
  PollBatch(BucketId bucket,
            std::optional<std::size_t> limit = std::nullopt) const;

  [[nodiscard]] BufferResult PeekInto(BucketId bucket,
                                      MutableBytesView output);
  [[nodiscard]] BufferResult PollInto(BucketId bucket,
                                      MutableBytesView output);
  [[nodiscard]] BufferResult
  PeekBatchInto(BucketId bucket, MutableBytesView output,
                std::optional<std::size_t> limit = std::nullopt);
  [[nodiscard]] BufferResult
  PollBatchInto(BucketId bucket, MutableBytesView output,
                std::optional<std::size_t> limit = std::nullopt);

  void Advance(BucketId bucket, BytesView key) const;
  [[nodiscard]] std::optional<std::vector<Byte>>
  Cursor(BucketId bucket) const;

private:
  struct Impl;
  explicit PriorityQueue(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class Db;
  friend class SingleDb;
};

} // namespace cobble::structured
