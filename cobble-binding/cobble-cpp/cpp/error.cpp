#include <cobble/types.hpp>

#include <utility>

#include "detail/error.hpp"

namespace cobble {

Error::Error(ErrorCode code, std::string message)
    : std::runtime_error(std::move(message)), code_(code) {}

ErrorCode Error::code() const noexcept { return code_; }

namespace detail {

ErrorCode ParseErrorCode(std::string_view message) noexcept {
  struct Prefix {
    std::string_view value;
    ErrorCode code;
  };
  static constexpr Prefix prefixes[] = {
      {"CB_URL:", ErrorCode::kUrl},
      {"CB_FILE_SYSTEM:", ErrorCode::kFileSystem},
      {"CB_IO:", ErrorCode::kIo},
      {"CB_MEMTABLE_FULL:", ErrorCode::kMemtableFull},
      {"CB_CONFIGURATION:", ErrorCode::kConfiguration},
      {"CB_INPUT:", ErrorCode::kInput},
      {"CB_COORDINATION:", ErrorCode::kCoordination},
      {"CB_INVALID_STATE:", ErrorCode::kInvalidState},
      {"CB_FILE_FORMAT:", ErrorCode::kFileFormat},
      {"CB_CHECKSUM:", ErrorCode::kChecksum},
      {"CB_CANCELLED:", ErrorCode::kCancelled},
  };
  for (const auto& prefix : prefixes) {
    if (message.starts_with(prefix.value)) {
      return prefix.code;
    }
  }
  return ErrorCode::kUnknown;
}

[[noreturn]] void ThrowTranslated(const rust::Error& error) {
  std::string message(error.what());
  const auto code = ParseErrorCode(message);
  throw Error(code, std::move(message));
}

}  // namespace detail
}  // namespace cobble
