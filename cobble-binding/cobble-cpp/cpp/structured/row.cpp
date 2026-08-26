#include <cobble/structured/row.hpp>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {

OwnedRow::OwnedRow(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedRow::OwnedRow(OwnedRow &&) noexcept = default;
OwnedRow &OwnedRow::operator=(OwnedRow &&) noexcept = default;
OwnedRow::~OwnedRow() = default;

bool OwnedRow::Found() const {
  return impl_ && structured_ffi::native_structured_row_found(*impl_->native);
}

std::size_t OwnedRow::ColumnCount() const {
  return impl_ ? structured_ffi::native_structured_row_column_count(
                     *impl_->native)
               : 0;
}

bool OwnedRow::HasColumn(std::size_t column) const {
  return impl_ && structured_ffi::native_structured_row_has_column(
                      *impl_->native, column);
}

ColumnKind OwnedRow::Kind(std::size_t column) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedRow has been moved from");
  }
  return static_cast<ColumnKind>(detail::Translate([&] {
    return structured_ffi::native_structured_row_kind(*impl_->native, column);
  }));
}

BytesView OwnedRow::Bytes(std::size_t column) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedRow has been moved from");
  }
  return detail::ToView(detail::Translate([&] {
    return structured_ffi::native_structured_row_bytes(*impl_->native, column);
  }));
}

std::size_t OwnedRow::ListSize(std::size_t column) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedRow has been moved from");
  }
  return detail::Translate([&] {
    return structured_ffi::native_structured_row_list_size(*impl_->native,
                                                           column);
  });
}

BytesView OwnedRow::ListElement(std::size_t column, std::size_t element) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedRow has been moved from");
  }
  return detail::ToView(detail::Translate([&] {
    return structured_ffi::native_structured_row_list_element(*impl_->native,
                                                              column, element);
  }));
}

} // namespace cobble::structured
