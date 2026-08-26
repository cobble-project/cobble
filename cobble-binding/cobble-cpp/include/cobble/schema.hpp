#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <cobble/types.hpp>

namespace cobble {

struct MergeOperatorSpec {
  std::string id;
  std::optional<std::string> metadata_json;
};

struct ColumnFamily {
  std::string name;
  std::uint8_t id;
  std::size_t column_count;
  bool value_has_ttl;
  // One entry per visible column, in column order.
  std::vector<MergeOperatorSpec> merge_operators;
};

struct Schema {
  std::uint64_t version;
  std::vector<ColumnFamily> column_families;
};

class COBBLE_CPP_API SchemaBuilder final {
 public:
  SchemaBuilder(SchemaBuilder&&) noexcept;
  SchemaBuilder& operator=(SchemaBuilder&&) noexcept;
  ~SchemaBuilder();

  SchemaBuilder(const SchemaBuilder&) = delete;
  SchemaBuilder& operator=(const SchemaBuilder&) = delete;

  void SetColumnOperator(std::optional<std::string> family,
                         std::size_t column,
                         const MergeOperatorSpec& merge_operator);
  // The optional default value is copied into the persisted schema evolution.
  void AddColumn(std::size_t column,
                 std::optional<MergeOperatorSpec> merge_operator = std::nullopt,
                 std::optional<BytesView> default_value = std::nullopt,
                 std::optional<std::string> family = std::nullopt);
  void DeleteColumn(std::optional<std::string> family, std::size_t column);
  void SetFamilyTtl(std::optional<std::string> family, bool value_has_ttl);
  // Commit consumes the native builder. Further calls fail with kInvalidState.
  [[nodiscard]] Schema Commit();

 private:
  struct Impl;
  explicit SchemaBuilder(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class Database;
};

}  // namespace cobble
