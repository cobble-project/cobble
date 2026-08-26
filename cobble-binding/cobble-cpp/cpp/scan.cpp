#include <cobble/scan.hpp>

#include <limits>
#include <utility>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"

namespace cobble {

OwnedBatch::OwnedBatch(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedBatch::OwnedBatch(OwnedBatch&&) noexcept = default;
OwnedBatch& OwnedBatch::operator=(OwnedBatch&&) noexcept = default;
OwnedBatch::~OwnedBatch() = default;

std::size_t OwnedBatch::row_count() const noexcept {
  if (!impl_) {
    return 0;
  }
  const auto count = ffi::native_batch_row_count(*impl_->native);
  return count > std::numeric_limits<std::size_t>::max()
             ? std::numeric_limits<std::size_t>::max()
             : static_cast<std::size_t>(count);
}

bool OwnedBatch::end() const noexcept {
  return impl_ && ffi::native_batch_end(*impl_->native);
}

bool OwnedBatch::stopped_at_block_boundary() const noexcept {
  return impl_ && ffi::native_batch_stopped_at_block_boundary(*impl_->native);
}

BucketId OwnedBatch::bucket(std::size_t row) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  }
  return detail::Translate(
      [&] { return ffi::native_batch_bucket(*impl_->native, row); });
}

BytesView OwnedBatch::key(std::size_t row) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  }
  return detail::Translate(
      [&] { return detail::ToView(ffi::native_batch_key(*impl_->native, row)); });
}

std::size_t OwnedBatch::column_count(std::size_t row) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  }
  return detail::ToSize(detail::Translate([&] {
                          return ffi::native_batch_column_count(*impl_->native, row);
                        }),
                        "column_count");
}

bool OwnedBatch::has_column(std::size_t row, std::size_t column) const {
  return impl_ && ffi::native_batch_has_column(*impl_->native, row, column);
}

BytesView OwnedBatch::column(std::size_t row, std::size_t column) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  }
  return detail::Translate([&] {
    return detail::ToView(ffi::native_batch_column(*impl_->native, row, column));
  });
}

ScanCursor::ScanCursor(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ScanCursor::ScanCursor(ScanCursor&&) noexcept = default;
ScanCursor& ScanCursor::operator=(ScanCursor&&) noexcept = default;
ScanCursor::~ScanCursor() = default;

OwnedBatch ScanCursor::Next(std::size_t max_rows) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ScanCursor has been moved from");
  }
  auto native = detail::Translate(
      [&] { return ffi::native_scan_cursor_next_owned(*impl_->native, max_rows); });
  return OwnedBatch(std::make_unique<OwnedBatch::Impl>(std::move(native)));
}

BufferResult ScanCursor::NextBatchInto(std::size_t max_rows,
                                       MutableBytesView output) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ScanCursor has been moved from");
  }
  return detail::ToBufferResult(detail::Translate([&] {
    return ffi::native_scan_cursor_next_batch_into(*impl_->native, max_rows,
                                                     detail::RustBytes(output));
  }));
}

void ScanCursor::ResumeAfterBlockBoundary() {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ScanCursor has been moved from");
  }
  detail::Translate([&] {
    ffi::native_scan_cursor_resume_after_block_boundary(*impl_->native);
  });
}

}  // namespace cobble
