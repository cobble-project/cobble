#include <cobble/database.hpp>

#include <string_view>
#include <utility>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"

namespace cobble {
namespace {

Schema ToSchema(const ffi::NativeSchema& native) {
  Schema result{native.version, {}};
  result.column_families.reserve(native.column_families.size());
  for (const auto& native_family : native.column_families) {
    ColumnFamily family{
        std::string(native_family.name),
        native_family.id,
        detail::ToSize(native_family.column_count, "schema column count"),
        native_family.value_has_ttl,
        {},
    };
    family.merge_operators.reserve(native_family.merge_operators.size());
    for (const auto& native_operator : native_family.merge_operators) {
      MergeOperatorSpec merge_operator{std::string(native_operator.id),
                                       std::nullopt};
      if (native_operator.has_metadata) {
        merge_operator.metadata_json =
            std::string(native_operator.metadata_json);
      }
      family.merge_operators.push_back(std::move(merge_operator));
    }
    result.column_families.push_back(std::move(family));
  }
  return result;
}

std::string_view OptionalStringView(const std::optional<std::string>& value) {
  return value ? std::string_view(*value) : std::string_view{};
}

}  // namespace

struct SchemaBuilder::Impl {
  explicit Impl(rust::Box<ffi::NativeSchemaBuilder> native_builder)
      : native(std::move(native_builder)) {}

  rust::Box<ffi::NativeSchemaBuilder> native;
};

SchemaBuilder::SchemaBuilder(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SchemaBuilder::SchemaBuilder(SchemaBuilder&&) noexcept = default;
SchemaBuilder& SchemaBuilder::operator=(SchemaBuilder&&) noexcept = default;
SchemaBuilder::~SchemaBuilder() = default;

void SchemaBuilder::SetColumnOperator(
    std::optional<std::string> family, std::size_t column,
    const MergeOperatorSpec& merge_operator) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "SchemaBuilder has already been consumed");
  }
  const auto family_view = OptionalStringView(family);
  const auto metadata_view = OptionalStringView(merge_operator.metadata_json);
  detail::Translate([&] {
    ffi::native_schema_builder_set_column_operator(
        *impl_->native, family.has_value(), detail::RustStr(family_view), column,
        detail::RustStr(merge_operator.id),
        merge_operator.metadata_json.has_value(),
        detail::RustStr(metadata_view));
  });
}

void SchemaBuilder::AddColumn(
    std::size_t column, std::optional<MergeOperatorSpec> merge_operator,
    std::optional<BytesView> default_value,
    std::optional<std::string> family) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "SchemaBuilder has already been consumed");
  }
  const auto family_view = OptionalStringView(family);
  const std::string_view operator_id =
      merge_operator ? std::string_view(merge_operator->id)
                     : std::string_view{};
  const std::optional<std::string> empty_metadata;
  const std::optional<std::string>& metadata =
      merge_operator ? merge_operator->metadata_json
                     : empty_metadata;
  const auto metadata_view = OptionalStringView(metadata);
  const auto default_view = default_value.value_or(BytesView{});
  detail::Translate([&] {
    ffi::native_schema_builder_add_column(
        *impl_->native, column, merge_operator.has_value(),
        detail::RustStr(operator_id), metadata.has_value(),
        detail::RustStr(metadata_view), default_value.has_value(),
        detail::RustBytes(default_view), family.has_value(),
        detail::RustStr(family_view));
  });
}

void SchemaBuilder::DeleteColumn(std::optional<std::string> family,
                                 std::size_t column) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "SchemaBuilder has already been consumed");
  }
  const auto family_view = OptionalStringView(family);
  detail::Translate([&] {
    ffi::native_schema_builder_delete_column(
        *impl_->native, family.has_value(), detail::RustStr(family_view), column);
  });
}

void SchemaBuilder::SetFamilyTtl(std::optional<std::string> family,
                                 bool value_has_ttl) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "SchemaBuilder has already been consumed");
  }
  const auto family_view = OptionalStringView(family);
  detail::Translate([&] {
    ffi::native_schema_builder_set_column_family_ttl(
        *impl_->native, family.has_value(), detail::RustStr(family_view),
        value_has_ttl);
  });
}

Schema SchemaBuilder::Commit() {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState,
                "SchemaBuilder has already been consumed");
  }
  auto impl = std::move(impl_);
  return ToSchema(detail::Translate([&] {
    return ffi::native_schema_builder_commit(std::move(impl->native));
  }));
}

Schema Database::CurrentSchema() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return ToSchema(detail::Translate(
      [&] { return ffi::native_database_current_schema(*impl_->native); }));
}

SchemaBuilder Database::UpdateSchema() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  auto native = ffi::native_database_update_schema(*impl_->native);
  return SchemaBuilder(
      std::make_unique<SchemaBuilder::Impl>(std::move(native)));
}

}  // namespace cobble
