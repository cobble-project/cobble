#include <cobble/structured/db.hpp>
#include <cobble/structured/scan.hpp>
#include <cobble/structured/single_db.hpp>

#include <limits>
#include <utility>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {

ScanOptions::ScanOptions()
    : impl_(std::make_unique<Impl>(
          structured_ffi::native_structured_scan_options_new())) {}
ScanOptions::ScanOptions(const ScanOptions &other)
    : impl_(other.impl_ ? std::make_unique<Impl>(detail::Translate([&] {
        return structured_ffi::native_structured_scan_options_clone(
            *other.impl_->native);
      }))
                        : nullptr) {}
ScanOptions &ScanOptions::operator=(const ScanOptions &other) {
  if (this == &other)
    return *this;
  ScanOptions copy(other);
  impl_.swap(copy.impl_);
  return *this;
}
ScanOptions::ScanOptions(ScanOptions &&) noexcept = default;
ScanOptions &ScanOptions::operator=(ScanOptions &&) noexcept = default;
ScanOptions::~ScanOptions() = default;

ScanOptions &
ScanOptions::SetColumnFamily(std::optional<std::string_view> family) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "ScanOptions has been moved from");
  const auto value = family.value_or(std::string_view{});
  detail::Translate([&] {
    structured_ffi::native_structured_scan_options_set_family(
        *impl_->native, family.has_value(), detail::RustStr(value));
  });
  return *this;
}

ScanOptions &ScanOptions::SetColumns(std::span<const std::size_t> columns) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "ScanOptions has been moved from");
  rust::Vec<std::uint64_t> native;
  native.reserve(columns.size());
  for (const auto column : columns) {
    if (column > std::numeric_limits<std::uint64_t>::max())
      throw Error(ErrorCode::kInput, "column index exceeds uint64_t");
    native.push_back(static_cast<std::uint64_t>(column));
  }
  detail::Translate([&] {
    structured_ffi::native_structured_scan_options_set_columns(
        *impl_->native, std::move(native));
  });
  return *this;
}

ScanOptions &ScanOptions::SetPreloadScanCursorBlock(bool enabled) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "ScanOptions has been moved from");
  structured_ffi::native_structured_scan_options_set_preload(*impl_->native,
                                                             enabled);
  return *this;
}

ScanOptions &ScanOptions::SetStopAtBlockBoundary(bool enabled) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "ScanOptions has been moved from");
  structured_ffi::native_structured_scan_options_set_stop_at_block_boundary(
      *impl_->native, enabled);
  return *this;
}

OwnedBatch::OwnedBatch(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedBatch::OwnedBatch(OwnedBatch &&) noexcept = default;
OwnedBatch &OwnedBatch::operator=(OwnedBatch &&) noexcept = default;
OwnedBatch::~OwnedBatch() = default;

std::size_t OwnedBatch::RowCount() const noexcept {
  return impl_
             ? structured_ffi::native_structured_batch_row_count(*impl_->native)
             : 0;
}
bool OwnedBatch::End() const noexcept {
  return impl_ && structured_ffi::native_structured_batch_end(*impl_->native);
}
bool OwnedBatch::StoppedAtBlockBoundary() const noexcept {
  return impl_ &&
         structured_ffi::native_structured_batch_stopped_at_block_boundary(
             *impl_->native);
}
BucketId OwnedBatch::Bucket(std::size_t row) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_batch_bucket(*impl_->native, row);
  });
}
BytesView OwnedBatch::Key(std::size_t row) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  return detail::ToView(detail::Translate([&] {
    return structured_ffi::native_structured_batch_key(*impl_->native, row);
  }));
}
std::size_t OwnedBatch::ColumnCount(std::size_t row) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_batch_column_count(*impl_->native,
                                                                row);
  });
}
bool OwnedBatch::HasColumn(std::size_t row, std::size_t column) const {
  return impl_ && structured_ffi::native_structured_batch_has_column(
                      *impl_->native, row, column);
}
ColumnKind OwnedBatch::Kind(std::size_t row, std::size_t column) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  return static_cast<ColumnKind>(detail::Translate([&] {
    return structured_ffi::native_structured_batch_kind(*impl_->native, row,
                                                        column);
  }));
}
BytesView OwnedBatch::Bytes(std::size_t row, std::size_t column) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  return detail::ToView(detail::Translate([&] {
    return structured_ffi::native_structured_batch_bytes(*impl_->native, row,
                                                         column);
  }));
}
std::size_t OwnedBatch::ListSize(std::size_t row, std::size_t column) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  return detail::Translate([&] {
    return structured_ffi::native_structured_batch_list_size(*impl_->native,
                                                             row, column);
  });
}
BytesView OwnedBatch::ListElement(std::size_t row, std::size_t column,
                                  std::size_t element) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  return detail::ToView(detail::Translate([&] {
    return structured_ffi::native_structured_batch_list_element(
        *impl_->native, row, column, element);
  }));
}

ScanCursor::ScanCursor(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ScanCursor::ScanCursor(ScanCursor &&) noexcept = default;
ScanCursor &ScanCursor::operator=(ScanCursor &&) noexcept = default;
ScanCursor::~ScanCursor() = default;

OwnedBatch ScanCursor::Next(std::size_t max_rows) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "ScanCursor has been moved from");
  if (max_rows > std::numeric_limits<std::uint64_t>::max())
    throw Error(ErrorCode::kInput, "max_rows exceeds uint64_t");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_scan_cursor_next_owned(
        *impl_->native, static_cast<std::uint64_t>(max_rows));
  });
  return OwnedBatch(std::make_unique<OwnedBatch::Impl>(std::move(native)));
}
BufferResult ScanCursor::NextBatchInto(std::size_t max_rows,
                                       MutableBytesView output) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "ScanCursor has been moved from");
  if (max_rows > std::numeric_limits<std::uint64_t>::max())
    throw Error(ErrorCode::kInput, "max_rows exceeds uint64_t");
  return detail::ToBufferResult(detail::Translate([&] {
    return structured_ffi::native_structured_scan_cursor_next_batch_into(
        *impl_->native, static_cast<std::uint64_t>(max_rows),
        rust::Slice<Byte>(output.data(), output.size()));
  }));
}
void ScanCursor::ResumeAfterBlockBoundary() {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "ScanCursor has been moved from");
  detail::Translate([&] {
    structured_ffi::native_structured_scan_cursor_resume_after_block_boundary(
        *impl_->native);
  });
}

ScanCursor Db::Scan(BucketId bucket, std::optional<BytesView> start_inclusive,
                    std::optional<BytesView> end_exclusive,
                    const ScanOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ScanOptions has been moved from");
  const auto start = start_inclusive.value_or(BytesView{});
  const auto end = end_exclusive.value_or(BytesView{});
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_db_scan(
        *impl_->native, bucket, detail::RustBytes(start),
        start_inclusive.has_value(), detail::RustBytes(end),
        end_exclusive.has_value(), *options.impl_->native);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

ScanCursor SingleDb::Scan(BucketId bucket,
                          std::optional<BytesView> start_inclusive,
                          std::optional<BytesView> end_exclusive,
                          const ScanOptions &options) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ScanOptions has been moved from");
  const auto start = start_inclusive.value_or(BytesView{});
  const auto end = end_exclusive.value_or(BytesView{});
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_single_db_scan(
        *impl_->native, bucket, detail::RustBytes(start),
        start_inclusive.has_value(), detail::RustBytes(end),
        end_exclusive.has_value(), *options.impl_->native);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

} // namespace cobble::structured
