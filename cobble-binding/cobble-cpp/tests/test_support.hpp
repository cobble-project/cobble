#pragma once

#include <cobble/cobble.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace cobble_test {

inline void Check(bool condition, std::string_view expression) {
  if (!condition) {
    throw std::runtime_error("check failed: " + std::string(expression));
  }
}

#define COBBLE_CHECK(condition) \
  ::cobble_test::Check(static_cast<bool>(condition), #condition)

inline cobble::BytesView Bytes(std::string_view value) {
  return {reinterpret_cast<const std::uint8_t*>(value.data()), value.size()};
}

inline std::string String(cobble::BytesView value) {
  return {reinterpret_cast<const char*>(value.data()), value.size()};
}

inline std::string FileUrl(const std::filesystem::path& path) {
  const auto generic = path.generic_string();
  return "file://" + std::string(generic.starts_with('/') ? "" : "/") + generic;
}

class TempDirectory {
 public:
  explicit TempDirectory(std::string_view prefix) {
    const auto nonce =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            (std::string(prefix) + "-" + std::to_string(nonce));
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }

  ~TempDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  TempDirectory(const TempDirectory&) = delete;
  TempDirectory& operator=(const TempDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

 private:
  std::filesystem::path path_;
};

// Reversible fixture rename used to prove metadata reads do not open data
// files.
class ScopedRename {
 public:
  ScopedRename(std::filesystem::path original, std::filesystem::path hidden)
      : original_(std::move(original)), hidden_(std::move(hidden)) {
    std::filesystem::rename(original_, hidden_);
    active_ = true;
  }

  ~ScopedRename() {
    if (active_) {
      std::error_code ignored;
      std::filesystem::rename(hidden_, original_, ignored);
    }
  }

  ScopedRename(const ScopedRename&) = delete;
  ScopedRename& operator=(const ScopedRename&) = delete;

  void Restore() {
    if (active_) {
      std::filesystem::rename(hidden_, original_);
      active_ = false;
    }
  }

 private:
  std::filesystem::path original_;
  std::filesystem::path hidden_;
  bool active_ = false;
};

}  // namespace cobble_test
