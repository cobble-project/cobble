#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <cobble/structured/types.hpp>

namespace cobble::structured {

struct ColumnType {
  ColumnIndex index = 0;
  ColumnKind kind = ColumnKind::kBytes;
  ListConfig list;
};

struct ColumnFamilySchema {
  std::string name;
  std::uint8_t id = 0;
  // Only non-default structured overrides need to be persisted. A column not
  // present here has BYTES semantics.
  std::vector<ColumnType> explicit_columns;
};

class COBBLE_CPP_API Schema final {
public:
  Schema() = default;
  explicit Schema(std::vector<ColumnFamilySchema> families);

  [[nodiscard]] const std::vector<ColumnFamilySchema> &
  Families() const noexcept;
  [[nodiscard]] ColumnType Type(std::string_view family,
                                ColumnIndex column) const;

private:
  std::vector<ColumnFamilySchema> families_;
};

class COBBLE_CPP_API SchemaBuilder final {
public:
  SchemaBuilder(SchemaBuilder &&) noexcept;
  SchemaBuilder &operator=(SchemaBuilder &&) noexcept;
  ~SchemaBuilder();

  SchemaBuilder(const SchemaBuilder &) = delete;
  SchemaBuilder &operator=(const SchemaBuilder &) = delete;

  SchemaBuilder &AddBytesColumn(std::optional<std::string_view> family,
                                ColumnIndex column);
  SchemaBuilder &AddListColumn(std::optional<std::string_view> family,
                               ColumnIndex column, const ListConfig &config);
  SchemaBuilder &DeleteColumn(std::optional<std::string_view> family,
                              ColumnIndex column);
  SchemaBuilder &SetFamilyTtl(std::optional<std::string_view> family,
                              bool value_has_ttl);
  [[nodiscard]] Schema Commit();

private:
  struct Impl;
  explicit SchemaBuilder(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class Db;
  friend class SingleDb;
};

} // namespace cobble::structured
