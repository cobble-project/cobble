#pragma once

#include <cobble/cobble.hpp>

#include "bridge.hpp"

namespace cobble::detail {

const char* NonNullData(std::string_view value) noexcept;
rust::Slice<const Byte> RustBytes(BytesView value) noexcept;
rust::Slice<Byte> RustBytes(MutableBytesView value) noexcept;
rust::Str RustStr(std::string_view value) noexcept;
rust::String RustString(std::string_view value);
std::size_t ToSize(std::uint64_t value, std::string_view field);
BytesView ToView(rust::Slice<const Byte> value) noexcept;
BufferStatus ToBufferStatus(std::uint8_t status);
BufferResult ToBufferResult(const ffi::NativeBufferResult& native);

}  // namespace cobble::detail
