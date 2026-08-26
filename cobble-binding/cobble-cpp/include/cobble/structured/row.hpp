#pragma once

#include <cstddef>
#include <memory>

#include <cobble/structured/types.hpp>

namespace cobble::structured {

// Move-only owner of Rust Bytes. Returned views remain valid until this row is
// destroyed or moved from, without copying BYTES or LIST element payloads.
class COBBLE_CPP_API OwnedRow final {
public:
  OwnedRow(OwnedRow &&) noexcept;
  OwnedRow &operator=(OwnedRow &&) noexcept;
  ~OwnedRow();

  OwnedRow(const OwnedRow &) = delete;
  OwnedRow &operator=(const OwnedRow &) = delete;

  [[nodiscard]] bool Found() const;
  [[nodiscard]] std::size_t ColumnCount() const;
  [[nodiscard]] bool HasColumn(std::size_t column) const;
  [[nodiscard]] ColumnKind Kind(std::size_t column) const;
  [[nodiscard]] BytesView Bytes(std::size_t column) const;
  [[nodiscard]] std::size_t ListSize(std::size_t column) const;
  [[nodiscard]] BytesView ListElement(std::size_t column,
                                      std::size_t element) const;

private:
  struct Impl;
  explicit OwnedRow(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class Db;
  friend class SingleDb;
};

} // namespace cobble::structured
