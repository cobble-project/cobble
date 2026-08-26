#include <cobble/structured/db.hpp>
#include <cobble/structured/multi_get.hpp>
#include <cobble/structured/single_db.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {
namespace {

constexpr std::size_t AlignUp(std::size_t value, std::size_t alignment) {
  return (value + alignment - 1) / alignment * alignment;
}

struct KeyDescriptor {
  std::uint16_t bucket;
  std::uint16_t reserved;
  const Byte *data;
  std::size_t length;
};

constexpr std::size_t kPointerOffset =
    AlignUp(sizeof(std::uint16_t) * 2, alignof(const Byte *));
constexpr std::size_t kLengthOffset =
    AlignUp(kPointerOffset + sizeof(const Byte *), alignof(std::size_t));
constexpr std::size_t kDescriptorAlignment = alignof(const Byte *) >
                                                     alignof(std::size_t)
                                                 ? alignof(const Byte *)
                                                 : alignof(std::size_t);
constexpr std::size_t kDescriptorSize =
    AlignUp(kLengthOffset + sizeof(std::size_t), kDescriptorAlignment);

static_assert(std::is_standard_layout_v<KeyDescriptor>);
static_assert(offsetof(KeyDescriptor, bucket) == 0);
static_assert(offsetof(KeyDescriptor, reserved) == sizeof(std::uint16_t));
static_assert(offsetof(KeyDescriptor, data) == kPointerOffset);
static_assert(offsetof(KeyDescriptor, length) == kLengthOffset);
static_assert(alignof(KeyDescriptor) == kDescriptorAlignment);
static_assert(sizeof(KeyDescriptor) == kDescriptorSize);

std::uint64_t Count(std::size_t value, const char *name) {
  if (value > std::numeric_limits<std::uint64_t>::max()) {
    throw Error(ErrorCode::kInput, std::string(name) + " exceeds uint64_t");
  }
  return static_cast<std::uint64_t>(value);
}

std::vector<KeyDescriptor> Descriptors(std::span<const MultiGetKey> keys) {
  std::vector<KeyDescriptor> descriptors;
  descriptors.reserve(keys.size());
  for (const auto &key : keys) {
    descriptors.push_back({key.bucket, 0, key.key.data(), key.key.size()});
  }
  return descriptors;
}

std::size_t Address(const std::vector<KeyDescriptor> &values) noexcept {
  return values.empty() ? 0 : reinterpret_cast<std::size_t>(values.data());
}

template <typename T>
void Require(const std::shared_ptr<T> &impl, const char *message) {
  if (!impl) {
    throw Error(ErrorCode::kInvalidState, message);
  }
}

} // namespace

OwnedMultiGetResult::OwnedMultiGetResult(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedMultiGetResult::OwnedMultiGetResult(OwnedMultiGetResult &&) noexcept =
    default;
OwnedMultiGetResult &
OwnedMultiGetResult::operator=(OwnedMultiGetResult &&) noexcept = default;
OwnedMultiGetResult::~OwnedMultiGetResult() = default;

std::size_t OwnedMultiGetResult::RowCount() const noexcept {
  return impl_ ? structured_ffi::native_structured_multi_get_row_count(
                     *impl_->native)
               : 0;
}

bool OwnedMultiGetResult::Found(std::size_t row) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "OwnedMultiGetResult has been moved from");
  return structured_ffi::native_structured_multi_get_found(*impl_->native, row);
}

std::size_t OwnedMultiGetResult::ColumnCount(std::size_t row) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "OwnedMultiGetResult has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_multi_get_column_count(
        *impl_->native, row);
  });
}

bool OwnedMultiGetResult::HasColumn(std::size_t row, std::size_t column) const {
  if (!impl_)
    return false;
  return structured_ffi::native_structured_multi_get_has_column(*impl_->native,
                                                                row, column);
}

ColumnKind OwnedMultiGetResult::Kind(std::size_t row,
                                     std::size_t column) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "OwnedMultiGetResult has been moved from");
  return static_cast<ColumnKind>(detail::Translate([&] {
    return structured_ffi::native_structured_multi_get_kind(*impl_->native, row,
                                                            column);
  }));
}

BytesView OwnedMultiGetResult::Bytes(std::size_t row,
                                     std::size_t column) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "OwnedMultiGetResult has been moved from");
  return detail::ToView(detail::Translate([&] {
    return structured_ffi::native_structured_multi_get_bytes(*impl_->native,
                                                             row, column);
  }));
}

std::size_t OwnedMultiGetResult::ListSize(std::size_t row,
                                          std::size_t column) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "OwnedMultiGetResult has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_multi_get_list_size(*impl_->native,
                                                                 row, column);
  });
}

BytesView OwnedMultiGetResult::ListElement(std::size_t row, std::size_t column,
                                           std::size_t element) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "OwnedMultiGetResult has been moved from");
  return detail::ToView(detail::Translate([&] {
    return structured_ffi::native_structured_multi_get_list_element(
        *impl_->native, row, column, element);
  }));
}

BufferResult Db::GetInto(BucketId bucket, BytesView key,
                         MutableBytesView output,
                         const ReadOptions &options) const {
  Require(impl_, "structured Db has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  return detail::ToBufferResult(detail::Translate([&] {
    return structured_ffi::native_structured_db_get_into(
        *impl_->native, bucket, detail::RustBytes(key), *options.impl_->native,
        rust::Slice<Byte>(output.data(), output.size()));
  }));
}

BufferResult Db::GetInto(BucketId bucket, BytesView key,
                         MutableBytesView output) const {
  const ReadOptions options;
  return GetInto(bucket, key, output, options);
}

OwnedMultiGetResult Db::MultiGet(std::span<const MultiGetKey> keys,
                                 const ReadOptions &options) const {
  Require(impl_, "structured Db has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  const auto descriptors = Descriptors(keys);
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_multi_get(
        *impl_->native, Address(descriptors), Count(keys.size(), "key count"),
        *options.impl_->native);
  });
  return OwnedMultiGetResult(
      std::make_unique<OwnedMultiGetResult::Impl>(std::move(native)));
}

OwnedMultiGetResult Db::MultiGet(std::span<const MultiGetKey> keys) const {
  const ReadOptions options;
  return MultiGet(keys, options);
}

BufferResult Db::MultiGetInto(std::span<const MultiGetKey> keys,
                              MutableBytesView output,
                              const ReadOptions &options) const {
  Require(impl_, "structured Db has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  const auto descriptors = Descriptors(keys);
  return detail::ToBufferResult(detail::Translate([&] {
    return structured_ffi::native_structured_db_multi_get_into(
        *impl_->native, Address(descriptors), Count(keys.size(), "key count"),
        *options.impl_->native,
        rust::Slice<Byte>(output.data(), output.size()));
  }));
}

BufferResult Db::MultiGetInto(std::span<const MultiGetKey> keys,
                              MutableBytesView output) const {
  const ReadOptions options;
  return MultiGetInto(keys, output, options);
}

BufferResult SingleDb::GetInto(BucketId bucket, BytesView key,
                               MutableBytesView output,
                               const ReadOptions &options) const {
  Require(impl_, "structured SingleDb has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  return detail::ToBufferResult(detail::Translate([&] {
    return structured_ffi::native_structured_single_db_get_into(
        *impl_->native, bucket, detail::RustBytes(key), *options.impl_->native,
        rust::Slice<Byte>(output.data(), output.size()));
  }));
}

BufferResult SingleDb::GetInto(BucketId bucket, BytesView key,
                               MutableBytesView output) const {
  const ReadOptions options;
  return GetInto(bucket, key, output, options);
}

OwnedMultiGetResult SingleDb::MultiGet(std::span<const MultiGetKey> keys,
                                       const ReadOptions &options) const {
  Require(impl_, "structured SingleDb has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  const auto descriptors = Descriptors(keys);
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_multi_get(
        *impl_->native, Address(descriptors), Count(keys.size(), "key count"),
        *options.impl_->native);
  });
  return OwnedMultiGetResult(
      std::make_unique<OwnedMultiGetResult::Impl>(std::move(native)));
}

OwnedMultiGetResult
SingleDb::MultiGet(std::span<const MultiGetKey> keys) const {
  const ReadOptions options;
  return MultiGet(keys, options);
}

BufferResult SingleDb::MultiGetInto(std::span<const MultiGetKey> keys,
                                    MutableBytesView output,
                                    const ReadOptions &options) const {
  Require(impl_, "structured SingleDb has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  const auto descriptors = Descriptors(keys);
  return detail::ToBufferResult(detail::Translate([&] {
    return structured_ffi::native_structured_single_db_multi_get_into(
        *impl_->native, Address(descriptors), Count(keys.size(), "key count"),
        *options.impl_->native,
        rust::Slice<Byte>(output.data(), output.size()));
  }));
}

BufferResult SingleDb::MultiGetInto(std::span<const MultiGetKey> keys,
                                    MutableBytesView output) const {
  const ReadOptions options;
  return MultiGetInto(keys, output, options);
}

} // namespace cobble::structured
