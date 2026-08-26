#pragma once

#include <cstddef>
#include <memory>
#include <span>

#include <cobble/options.hpp>
#include <cobble/types.hpp>

namespace cobble {

struct MultiGetKey {
  BucketId bucket;
  BytesView key;
};

class COBBLE_CPP_API OwnedMultiGetResult final {
 public:
  OwnedMultiGetResult(OwnedMultiGetResult&&) noexcept;
  OwnedMultiGetResult& operator=(OwnedMultiGetResult&&) noexcept;
  ~OwnedMultiGetResult();
  OwnedMultiGetResult(const OwnedMultiGetResult&) = delete;
  OwnedMultiGetResult& operator=(const OwnedMultiGetResult&) = delete;
  [[nodiscard]] std::size_t row_count() const noexcept;
  [[nodiscard]] bool found(std::size_t row) const;
  [[nodiscard]] std::size_t column_count(std::size_t row) const;
  [[nodiscard]] bool has_column(std::size_t row, std::size_t column) const;
  // The view remains valid while this result is alive and is not moved.
  [[nodiscard]] BytesView column(std::size_t row, std::size_t column) const;

 private:
  struct Impl;
  explicit OwnedMultiGetResult(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
  friend class Database;
};

}  // namespace cobble
