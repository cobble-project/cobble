#include <cobble/structured/options.hpp>

#include <limits>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {

ReadOptions::ReadOptions()
    : impl_(std::make_unique<Impl>(
          structured_ffi::native_structured_read_options_new())) {}

ReadOptions::ReadOptions(const ReadOptions &other)
    : impl_(other.impl_ ? std::make_unique<Impl>(detail::Translate([&] {
        return structured_ffi::native_structured_read_options_clone(
            *other.impl_->native);
      }))
                        : nullptr) {}

ReadOptions &ReadOptions::operator=(const ReadOptions &other) {
  if (this == &other) {
    return *this;
  }
  ReadOptions copy(other);
  impl_.swap(copy.impl_);
  return *this;
}

ReadOptions::ReadOptions(ReadOptions &&) noexcept = default;
ReadOptions &ReadOptions::operator=(ReadOptions &&) noexcept = default;
ReadOptions::~ReadOptions() = default;

ReadOptions &
ReadOptions::SetColumnFamily(std::optional<std::string_view> family) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  }
  const auto value = family.value_or(std::string_view{});
  detail::Translate([&] {
    structured_ffi::native_structured_read_options_set_family(
        *impl_->native, family.has_value(), detail::RustStr(value));
  });
  return *this;
}

ReadOptions &ReadOptions::SetColumns(std::span<const std::size_t> columns) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ReadOptions has been moved from");
  }
  rust::Vec<std::uint64_t> native;
  native.reserve(columns.size());
  for (const auto column : columns) {
    if (column > std::numeric_limits<std::uint64_t>::max()) {
      throw Error(ErrorCode::kInput, "column index exceeds uint64_t");
    }
    native.push_back(static_cast<std::uint64_t>(column));
  }
  detail::Translate([&] {
    structured_ffi::native_structured_read_options_set_columns(
        *impl_->native, std::move(native));
  });
  return *this;
}

} // namespace cobble::structured
