#include <cobble/write_batch.hpp>

#include <limits>
#include <utility>

#include "detail/convert.hpp"
#include "detail/impl.hpp"
#include "detail/options.hpp"

namespace cobble {

WriteBatch::WriteBatch() : impl_(std::make_unique<Impl>()) {}
WriteBatch::WriteBatch(WriteBatch&&) noexcept = default;
WriteBatch& WriteBatch::operator=(WriteBatch&&) noexcept = default;
WriteBatch::~WriteBatch() = default;

void WriteBatch::Put(BucketId bucket, BytesView key, ColumnIndex column,
                     BytesView value, const WriteOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  ffi::native_write_batch_put(*impl_->native, bucket, detail::RustBytes(key),
                              column, detail::RustBytes(value), native_options);
}

void WriteBatch::Delete(BucketId bucket, BytesView key, ColumnIndex column,
                        const WriteOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  ffi::native_write_batch_delete(*impl_->native, bucket, detail::RustBytes(key),
                                 column, native_options);
}

void WriteBatch::Merge(BucketId bucket, BytesView key, ColumnIndex column,
                       BytesView value, const WriteOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  }
  const auto native_options = detail::ToNative(options);
  ffi::native_write_batch_merge(*impl_->native, bucket, detail::RustBytes(key),
                                column, detail::RustBytes(value), native_options);
}

std::size_t WriteBatch::size() const noexcept {
  if (!impl_) {
    return 0;
  }
  const auto count = ffi::native_write_batch_len(*impl_->native);
  return count > std::numeric_limits<std::size_t>::max()
             ? std::numeric_limits<std::size_t>::max()
             : static_cast<std::size_t>(count);
}

}  // namespace cobble
