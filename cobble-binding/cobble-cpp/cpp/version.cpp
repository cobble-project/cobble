#include <cobble/database.hpp>

#include <string>

#include "detail/bridge.hpp"

namespace cobble {

std::string_view Version() noexcept {
  static const std::string version = [] {
    try {
      const auto native = ffi::native_database_version();
      return std::string(native.data(), native.size());
    } catch (...) {
      return std::string("unknown");
    }
  }();
  return version;
}

}  // namespace cobble
