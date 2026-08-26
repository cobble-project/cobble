#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace cobble {

struct ReadOptions {
  std::optional<std::string> column_family;
  // Empty means all columns. Values are returned in this order.
  std::vector<std::size_t> columns;
};

struct WriteOptions {
  std::optional<std::uint32_t> ttl_seconds;
  std::optional<std::string> column_family;
  bool await_durable = true;
};

struct ScanOptions {
  std::optional<std::string> column_family;
  // Empty means all columns. Values are returned in this order.
  std::vector<std::size_t> columns;
  std::size_t read_ahead_bytes = 0;
  std::optional<std::size_t> max_rows;
  bool preload_scan_cursor_block = false;
  bool stop_at_block_boundary = false;
};

}  // namespace cobble
