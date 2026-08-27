#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

#include <cobble/types.hpp>

namespace cobble::structured {

class Db;
class SingleDb;

enum class ColumnKind : std::uint8_t {
  kBytes = 0,
  kList = 1,
};

enum class ListRetainMode : std::uint8_t {
  kFirst = 0,
  kLast = 1,
};

struct ListConfig {
  std::optional<std::size_t> max_elements;
  ListRetainMode retain_mode = ListRetainMode::kLast;
  bool preserve_element_ttl = false;
};

} // namespace cobble::structured
