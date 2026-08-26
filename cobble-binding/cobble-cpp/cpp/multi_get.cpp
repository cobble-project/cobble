#include <cobble/database.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"
#include "detail/options.hpp"

namespace cobble {
namespace {

// This private descriptor is borrowed by Rust only for the synchronous bridge
// call. Payload bytes are never concatenated or copied.
struct KeyDescriptor {
  std::uint16_t bucket;
  std::uint16_t padding;
  const Byte* data;
  std::size_t length;
};

static_assert(std::is_standard_layout_v<KeyDescriptor>);
constexpr std::size_t AlignUp(std::size_t value, std::size_t alignment) {
  return (value + alignment - 1) / alignment * alignment;
}
constexpr std::size_t kDescriptorAlignment =
    alignof(const Byte*) > alignof(std::size_t) ? alignof(const Byte*)
                                                : alignof(std::size_t);
constexpr std::size_t kDataOffset =
    AlignUp(2 * sizeof(std::uint16_t), alignof(const Byte*));
constexpr std::size_t kLengthOffset =
    AlignUp(kDataOffset + sizeof(const Byte*), alignof(std::size_t));
constexpr std::size_t kDescriptorSize =
    AlignUp(kLengthOffset + sizeof(std::size_t), kDescriptorAlignment);
static_assert(offsetof(KeyDescriptor, bucket) == 0);
static_assert(offsetof(KeyDescriptor, padding) == sizeof(std::uint16_t));
static_assert(offsetof(KeyDescriptor, data) == kDataOffset);
static_assert(offsetof(KeyDescriptor, length) == kLengthOffset);
static_assert(alignof(KeyDescriptor) == kDescriptorAlignment);
static_assert(sizeof(KeyDescriptor) == kDescriptorSize);

template <typename Call>
rust::Box<ffi::NativeMultiGetResult> CallMultiGet(
    std::span<const MultiGetKey> keys, const ReadOptions& options, Call&& call) {
  if (keys.size() > std::numeric_limits<std::uint64_t>::max()) {
    throw Error(ErrorCode::kInput, "multi-get key count does not fit in u64");
  }

  std::vector<KeyDescriptor> descriptors;
  descriptors.reserve(keys.size());
  for (const auto& key : keys) {
    descriptors.push_back({key.bucket, 0, key.key.data(), key.key.size()});
  }
  const auto native_options = detail::ToNative(options);
  return detail::Translate([&] {
    return call(reinterpret_cast<std::size_t>(descriptors.data()),
                descriptors.size(), native_options);
  });
}

}  // namespace

struct OwnedMultiGetResult::Impl {
  explicit Impl(rust::Box<ffi::NativeMultiGetResult> native_result)
      : native(std::move(native_result)) {}

  rust::Box<ffi::NativeMultiGetResult> native;
};

OwnedMultiGetResult::OwnedMultiGetResult(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedMultiGetResult::OwnedMultiGetResult(OwnedMultiGetResult&&) noexcept =
    default;
OwnedMultiGetResult& OwnedMultiGetResult::operator=(
    OwnedMultiGetResult&&) noexcept = default;
OwnedMultiGetResult::~OwnedMultiGetResult() = default;

std::size_t OwnedMultiGetResult::row_count() const noexcept {
  if (!impl_) {
    return 0;
  }
  const auto count = ffi::native_multi_get_row_count(*impl_->native);
  return count > std::numeric_limits<std::size_t>::max()
             ? std::numeric_limits<std::size_t>::max()
             : static_cast<std::size_t>(count);
}

bool OwnedMultiGetResult::found(std::size_t row) const {
  return impl_ && ffi::native_multi_get_found(*impl_->native, row);
}

std::size_t OwnedMultiGetResult::column_count(std::size_t row) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "OwnedMultiGetResult has been moved from");
  }
  return detail::ToSize(detail::Translate([&] {
                          return ffi::native_multi_get_column_count(
                              *impl_->native, row);
                        }),
                        "multi-get column count");
}

bool OwnedMultiGetResult::has_column(std::size_t row,
                                     std::size_t column) const {
  return impl_ &&
         ffi::native_multi_get_has_column(*impl_->native, row, column);
}

BytesView OwnedMultiGetResult::column(std::size_t row,
                                      std::size_t column) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "OwnedMultiGetResult has been moved from");
  }
  return detail::Translate([&] {
    return detail::ToView(
        ffi::native_multi_get_column(*impl_->native, row, column));
  });
}

OwnedMultiGetResult Database::MultiGet(
    std::span<const MultiGetKey> keys, const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  auto native = CallMultiGet(keys, options, [&](auto address, auto count,
                                                const auto& native_options) {
    return ffi::native_database_multi_get(
        *impl_->native, address, count, native_options);
  });
  return OwnedMultiGetResult(
      std::make_unique<OwnedMultiGetResult::Impl>(std::move(native)));
}

OwnedMultiGetResult Db::MultiGet(std::span<const MultiGetKey> keys,
                                 const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Db has been moved from");
  }
  auto native = CallMultiGet(keys, options, [&](auto address, auto count,
                                                const auto& native_options) {
    return ffi::native_sharded_database_multi_get(
        *impl_->native, address, count, native_options);
  });
  return OwnedMultiGetResult(
      std::make_unique<OwnedMultiGetResult::Impl>(std::move(native)));
}

OwnedMultiGetResult ReadOnlyDb::MultiGet(
    std::span<const MultiGetKey> keys, const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ReadOnlyDb has been moved from");
  }
  auto native = CallMultiGet(keys, options, [&](auto address, auto count,
                                                const auto& native_options) {
    return ffi::native_read_only_database_multi_get(
        *impl_->native, address, count, native_options);
  });
  return OwnedMultiGetResult(
      std::make_unique<OwnedMultiGetResult::Impl>(std::move(native)));
}

OwnedMultiGetResult Reader::MultiGet(std::span<const MultiGetKey> keys,
                                     const ReadOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Reader has been moved from");
  }
  auto native = CallMultiGet(keys, options, [&](auto address, auto count,
                                                const auto& native_options) {
    return ffi::native_reader_multi_get(*impl_->native, address, count,
                                        native_options);
  });
  return OwnedMultiGetResult(
      std::make_unique<OwnedMultiGetResult::Impl>(std::move(native)));
}

}  // namespace cobble
