#include <cobble/structured/db.hpp>
#include <cobble/structured/priority_queue.hpp>
#include <cobble/structured/single_db.hpp>

#include <utility>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {
namespace {

template <typename T>
void Require(const std::unique_ptr<T> &value, const char *message) {
  if (!value)
    throw Error(ErrorCode::kInvalidState, message);
}

template <typename T>
void Require(const std::shared_ptr<T> &value, const char *message) {
  if (!value)
    throw Error(ErrorCode::kInvalidState, message);
}

template <typename T>
void RequireExclusiveOwner(const std::shared_ptr<T> &value) {
  if (value.use_count() != 1) {
    throw Error(ErrorCode::kInvalidState,
                "priority queue creation requires releasing every scan "
                "cursor, schema builder, and priority queue first");
  }
}

std::pair<bool, std::size_t>
NativeLimit(std::optional<std::size_t> limit) noexcept {
  return {limit.has_value(), limit.value_or(0)};
}

} // namespace

OwnedPriorityQueueEntry::OwnedPriorityQueueEntry(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedPriorityQueueEntry::OwnedPriorityQueueEntry(
    OwnedPriorityQueueEntry &&) noexcept = default;
OwnedPriorityQueueEntry &OwnedPriorityQueueEntry::operator=(
    OwnedPriorityQueueEntry &&) noexcept = default;
OwnedPriorityQueueEntry::~OwnedPriorityQueueEntry() = default;

BytesView OwnedPriorityQueueEntry::Key() const {
  Require(impl_, "OwnedPriorityQueueEntry has been moved from");
  return detail::ToView(detail::Translate([&] {
    return structured_ffi::native_priority_queue_batch_key(*impl_->native, 0);
  }));
}

BytesView OwnedPriorityQueueEntry::Value() const {
  Require(impl_, "OwnedPriorityQueueEntry has been moved from");
  return detail::ToView(detail::Translate([&] {
    return structured_ffi::native_priority_queue_batch_value(*impl_->native,
                                                             0);
  }));
}

OwnedPriorityQueueBatch::OwnedPriorityQueueBatch(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedPriorityQueueBatch::OwnedPriorityQueueBatch(
    OwnedPriorityQueueBatch &&) noexcept = default;
OwnedPriorityQueueBatch &OwnedPriorityQueueBatch::operator=(
    OwnedPriorityQueueBatch &&) noexcept = default;
OwnedPriorityQueueBatch::~OwnedPriorityQueueBatch() = default;

std::size_t OwnedPriorityQueueBatch::Size() const noexcept {
  return impl_ ? structured_ffi::native_priority_queue_batch_size(*impl_->native)
               : 0;
}

PriorityQueueEntryView
OwnedPriorityQueueBatch::Entry(std::size_t index) const {
  Require(impl_, "OwnedPriorityQueueBatch has been moved from");
  return PriorityQueueEntryView{
      detail::ToView(detail::Translate([&] {
        return structured_ffi::native_priority_queue_batch_key(*impl_->native,
                                                               index);
      })),
      detail::ToView(detail::Translate([&] {
        return structured_ffi::native_priority_queue_batch_value(
            *impl_->native, index);
      }))};
}

PriorityQueue::PriorityQueue(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
PriorityQueue::PriorityQueue(PriorityQueue &&) noexcept = default;
PriorityQueue &PriorityQueue::operator=(PriorityQueue &&) noexcept = default;
PriorityQueue::~PriorityQueue() = default;

std::string PriorityQueue::ColumnFamily() const {
  Require(impl_, "PriorityQueue has been moved from");
  return std::string(
      structured_ffi::native_structured_priority_queue_column_family(
          *impl_->native));
}

void PriorityQueue::Offer(BucketId bucket, BytesView key,
                          BytesView value) const {
  Require(impl_, "PriorityQueue has been moved from");
  detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_offer(
        *impl_->native, bucket, detail::RustBytes(key),
        detail::RustBytes(value));
  });
}

void PriorityQueue::Delete(BucketId bucket, BytesView key) const {
  Require(impl_, "PriorityQueue has been moved from");
  detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_delete(
        *impl_->native, bucket, detail::RustBytes(key));
  });
}

std::optional<OwnedPriorityQueueEntry>
PriorityQueue::Peek(BucketId bucket) const {
  Require(impl_, "PriorityQueue has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_peek(*impl_->native,
                                                                 bucket);
  });
  if (structured_ffi::native_priority_queue_batch_size(*native) == 0)
    return std::nullopt;
  OwnedPriorityQueueEntry entry(
      std::make_unique<OwnedPriorityQueueEntry::Impl>(std::move(native)));
  return std::optional<OwnedPriorityQueueEntry>(std::move(entry));
}

std::optional<OwnedPriorityQueueEntry>
PriorityQueue::Poll(BucketId bucket) const {
  Require(impl_, "PriorityQueue has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_poll(*impl_->native,
                                                                 bucket);
  });
  if (structured_ffi::native_priority_queue_batch_size(*native) == 0)
    return std::nullopt;
  OwnedPriorityQueueEntry entry(
      std::make_unique<OwnedPriorityQueueEntry::Impl>(std::move(native)));
  return std::optional<OwnedPriorityQueueEntry>(std::move(entry));
}

OwnedPriorityQueueBatch
PriorityQueue::PeekBatch(BucketId bucket,
                         std::optional<std::size_t> limit) const {
  Require(impl_, "PriorityQueue has been moved from");
  const auto [has_limit, native_limit] = NativeLimit(limit);
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_peek_batch(
        *impl_->native, bucket, has_limit, native_limit);
  });
  return OwnedPriorityQueueBatch(
      std::make_unique<OwnedPriorityQueueBatch::Impl>(std::move(native)));
}

OwnedPriorityQueueBatch
PriorityQueue::PollBatch(BucketId bucket,
                         std::optional<std::size_t> limit) const {
  Require(impl_, "PriorityQueue has been moved from");
  const auto [has_limit, native_limit] = NativeLimit(limit);
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_poll_batch(
        *impl_->native, bucket, has_limit, native_limit);
  });
  return OwnedPriorityQueueBatch(
      std::make_unique<OwnedPriorityQueueBatch::Impl>(std::move(native)));
}

BufferResult PriorityQueue::PeekInto(BucketId bucket,
                                     MutableBytesView output) {
  Require(impl_, "PriorityQueue has been moved from");
  return detail::ToBufferResult(detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_peek_into(
        *impl_->native, bucket,
        rust::Slice<Byte>(output.data(), output.size()));
  }));
}

BufferResult PriorityQueue::PollInto(BucketId bucket,
                                     MutableBytesView output) {
  Require(impl_, "PriorityQueue has been moved from");
  return detail::ToBufferResult(detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_poll_into(
        *impl_->native, bucket,
        rust::Slice<Byte>(output.data(), output.size()));
  }));
}

BufferResult
PriorityQueue::PeekBatchInto(BucketId bucket, MutableBytesView output,
                             std::optional<std::size_t> limit) {
  Require(impl_, "PriorityQueue has been moved from");
  const auto [has_limit, native_limit] = NativeLimit(limit);
  return detail::ToBufferResult(detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_peek_batch_into(
        *impl_->native, bucket, has_limit, native_limit,
        rust::Slice<Byte>(output.data(), output.size()));
  }));
}

BufferResult
PriorityQueue::PollBatchInto(BucketId bucket, MutableBytesView output,
                             std::optional<std::size_t> limit) {
  Require(impl_, "PriorityQueue has been moved from");
  const auto [has_limit, native_limit] = NativeLimit(limit);
  return detail::ToBufferResult(detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_poll_batch_into(
        *impl_->native, bucket, has_limit, native_limit,
        rust::Slice<Byte>(output.data(), output.size()));
  }));
}

void PriorityQueue::Advance(BucketId bucket, BytesView key) const {
  Require(impl_, "PriorityQueue has been moved from");
  detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_advance(
        *impl_->native, bucket, detail::RustBytes(key));
  });
}

std::optional<std::vector<Byte>>
PriorityQueue::Cursor(BucketId bucket) const {
  Require(impl_, "PriorityQueue has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_priority_queue_cursor(
        *impl_->native, bucket);
  });
  if (!structured_ffi::native_priority_queue_cursor_has_value(*native))
    return std::nullopt;
  const auto value = detail::Translate([&] {
    return structured_ffi::native_priority_queue_cursor_value(*native);
  });
  if (value.empty())
    return std::vector<Byte>{};
  return std::vector<Byte>(value.begin(), value.end());
}

PriorityQueue Db::NewPriorityQueue(std::string_view name) {
  Require(impl_, "structured Db has been moved from");
  RequireExclusiveOwner(impl_);
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_new_priority_queue(
        *impl_->native, detail::RustStr(name));
  });
  PriorityQueue::Impl::Owner owner = impl_;
  return PriorityQueue(std::make_unique<PriorityQueue::Impl>(
      std::move(owner), std::move(native)));
}

PriorityQueue Db::GetPriorityQueue(std::string_view name) const {
  Require(impl_, "structured Db has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_get_priority_queue(
        *impl_->native, detail::RustStr(name));
  });
  PriorityQueue::Impl::Owner owner = impl_;
  return PriorityQueue(std::make_unique<PriorityQueue::Impl>(
      std::move(owner), std::move(native)));
}

PriorityQueue Db::GetOrNewPriorityQueue(std::string_view name) {
  Require(impl_, "structured Db has been moved from");
  RequireExclusiveOwner(impl_);
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_get_or_new_priority_queue(
        *impl_->native, detail::RustStr(name));
  });
  PriorityQueue::Impl::Owner owner = impl_;
  return PriorityQueue(std::make_unique<PriorityQueue::Impl>(
      std::move(owner), std::move(native)));
}

PriorityQueue SingleDb::NewPriorityQueue(std::string_view name) {
  Require(impl_, "structured SingleDb has been moved from");
  RequireExclusiveOwner(impl_);
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_new_priority_queue(
        *impl_->native, detail::RustStr(name));
  });
  PriorityQueue::Impl::Owner owner = impl_;
  return PriorityQueue(std::make_unique<PriorityQueue::Impl>(
      std::move(owner), std::move(native)));
}

PriorityQueue SingleDb::GetPriorityQueue(std::string_view name) const {
  Require(impl_, "structured SingleDb has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_get_priority_queue(
        *impl_->native, detail::RustStr(name));
  });
  PriorityQueue::Impl::Owner owner = impl_;
  return PriorityQueue(std::make_unique<PriorityQueue::Impl>(
      std::move(owner), std::move(native)));
}

PriorityQueue SingleDb::GetOrNewPriorityQueue(std::string_view name) {
  Require(impl_, "structured SingleDb has been moved from");
  RequireExclusiveOwner(impl_);
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_get_or_new_priority_queue(
        *impl_->native, detail::RustStr(name));
  });
  PriorityQueue::Impl::Owner owner = impl_;
  return PriorityQueue(std::make_unique<PriorityQueue::Impl>(
      std::move(owner), std::move(native)));
}

} // namespace cobble::structured
