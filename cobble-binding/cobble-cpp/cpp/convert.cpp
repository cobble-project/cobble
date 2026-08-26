#include "detail/convert.hpp"

#include <limits>

namespace cobble::detail {

const Byte* NonNullData(BytesView value) noexcept {
  static constexpr Byte kEmpty = 0;
  return value.empty() ? &kEmpty : value.data();
}

Byte* NonNullData(MutableBytesView value) noexcept {
  static Byte empty = 0;
  return value.empty() ? &empty : value.data();
}

const char* NonNullData(std::string_view value) noexcept {
  static constexpr char kEmpty = '\0';
  return value.empty() ? &kEmpty : value.data();
}

rust::Slice<const Byte> RustBytes(BytesView value) noexcept {
  return {NonNullData(value), value.size()};
}

rust::Slice<Byte> RustBytes(MutableBytesView value) noexcept {
  return {NonNullData(value), value.size()};
}

rust::Str RustStr(std::string_view value) noexcept {
  return {NonNullData(value), value.size()};
}

rust::String RustString(std::string_view value) {
  return {NonNullData(value), value.size()};
}

std::size_t ToSize(std::uint64_t value, std::string_view field) {
  if (value > std::numeric_limits<std::size_t>::max()) {
    throw Error(ErrorCode::kInvalidState,
                std::string(field) + " exceeds the C++ address space");
  }
  return static_cast<std::size_t>(value);
}

BytesView ToView(rust::Slice<const Byte> value) noexcept {
  return {value.data(), value.size()};
}

BufferStatus ToBufferStatus(std::uint8_t status) {
  switch (status) {
    case 0:
      return BufferStatus::kOk;
    case 1:
      return BufferStatus::kNotFound;
    case 2:
      return BufferStatus::kEnd;
    case 3:
      return BufferStatus::kBufferTooSmall;
    case 4:
      return BufferStatus::kBlockBoundary;
    default:
      throw Error(ErrorCode::kInvalidState,
                  "Rust bridge returned an unknown buffer status");
  }
}

BufferResult ToBufferResult(const ffi::NativeBufferResult& native) {
  return {ToBufferStatus(native.status), ToSize(native.bytes_written, "bytes_written"),
          ToSize(native.bytes_required, "bytes_required"),
          ToSize(native.row_count, "row_count")};
}

rust::Vec<ffi::NativeRange> ToNativeRanges(
    std::span<const BucketRange> ranges) {
  rust::Vec<ffi::NativeRange> native;
  native.reserve(ranges.size());
  for (const auto& range : ranges) {
    ffi::NativeRange value;
    value.first = range.start_inclusive;
    value.last = range.end_inclusive;
    native.push_back(value);
  }
  return native;
}

}  // namespace cobble::detail
