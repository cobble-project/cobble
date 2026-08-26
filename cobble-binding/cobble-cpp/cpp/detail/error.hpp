#pragma once

#include <utility>

#include "convert.hpp"

namespace cobble::detail {

ErrorCode ParseErrorCode(std::string_view message) noexcept;
[[noreturn]] void ThrowTranslated(const rust::Error& error);

template <typename Function>
decltype(auto) Translate(Function&& function) {
  try {
    return std::forward<Function>(function)();
  } catch (const rust::Error& error) {
    ThrowTranslated(error);
  }
}

}  // namespace cobble::detail
