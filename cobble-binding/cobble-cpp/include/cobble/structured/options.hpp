#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include <cobble/structured/types.hpp>

namespace cobble::structured {

// Native-backed and reusable. Reusing an instance preserves the Rust schema
// projection cache across Get calls.
class COBBLE_CPP_API ReadOptions final {
public:
  ReadOptions();
  ReadOptions(const ReadOptions &other);
  ReadOptions &operator=(const ReadOptions &other);
  ReadOptions(ReadOptions &&) noexcept;
  ReadOptions &operator=(ReadOptions &&) noexcept;
  ~ReadOptions();

  ReadOptions &SetColumnFamily(std::optional<std::string_view> family);
  ReadOptions &SetColumns(std::span<const std::size_t> columns);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  friend class Db;
  friend class SingleDb;
};

} // namespace cobble::structured
