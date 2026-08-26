#pragma once

#include <cstddef>
#include <memory>

#include <cobble/structured/row.hpp>

namespace cobble::structured {

struct MultiGetKey {
  BucketId bucket;
  BytesView key;
};

class COBBLE_CPP_API OwnedMultiGetResult final {
public:
  OwnedMultiGetResult(OwnedMultiGetResult &&) noexcept;
  OwnedMultiGetResult &operator=(OwnedMultiGetResult &&) noexcept;
  ~OwnedMultiGetResult();
  OwnedMultiGetResult(const OwnedMultiGetResult &) = delete;
  OwnedMultiGetResult &operator=(const OwnedMultiGetResult &) = delete;

  [[nodiscard]] std::size_t RowCount() const noexcept;
  [[nodiscard]] bool Found(std::size_t row) const;
  [[nodiscard]] std::size_t ColumnCount(std::size_t row) const;
  [[nodiscard]] bool HasColumn(std::size_t row, std::size_t column) const;
  [[nodiscard]] ColumnKind Kind(std::size_t row, std::size_t column) const;
  [[nodiscard]] BytesView Bytes(std::size_t row, std::size_t column) const;
  [[nodiscard]] std::size_t ListSize(std::size_t row, std::size_t column) const;
  [[nodiscard]] BytesView ListElement(std::size_t row, std::size_t column,
                                      std::size_t element) const;

private:
  struct Impl;
  explicit OwnedMultiGetResult(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
  friend class Db;
  friend class SingleDb;
};

} // namespace cobble::structured
