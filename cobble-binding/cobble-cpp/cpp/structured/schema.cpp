#include <cobble/structured/schema.hpp>

#include <algorithm>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {

Schema::Schema(std::vector<ColumnFamilySchema> families)
    : families_(std::move(families)) {}

const std::vector<ColumnFamilySchema> &Schema::Families() const noexcept {
  return families_;
}

ColumnType Schema::Type(std::string_view family, ColumnIndex column) const {
  const auto family_it =
      std::find_if(families_.begin(), families_.end(),
                   [&](const auto &value) { return value.name == family; });
  if (family_it == families_.end()) {
    throw Error(ErrorCode::kInput, "unknown structured column family");
  }
  const auto column_it = std::find_if(
      family_it->explicit_columns.begin(), family_it->explicit_columns.end(),
      [&](const auto &value) { return value.index == column; });
  if (column_it == family_it->explicit_columns.end()) {
    return ColumnType{.index = column, .kind = ColumnKind::kBytes};
  }
  return *column_it;
}

SchemaBuilder::SchemaBuilder(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SchemaBuilder::SchemaBuilder(SchemaBuilder &&) noexcept = default;
SchemaBuilder &SchemaBuilder::operator=(SchemaBuilder &&) noexcept = default;
SchemaBuilder::~SchemaBuilder() = default;

namespace {

std::string_view FamilyValue(std::optional<std::string_view> family) {
  return family.value_or(std::string_view{});
}

void CheckBuilder(const auto *impl) {
  if (impl == nullptr || impl->committed || !impl->native.has_value()) {
    throw Error(ErrorCode::kInvalidState,
                "SchemaBuilder has been moved from or committed");
  }
}

} // namespace

SchemaBuilder &
SchemaBuilder::AddBytesColumn(std::optional<std::string_view> family,
                              ColumnIndex column) {
  CheckBuilder(impl_.get());
  detail::Translate([&] {
    structured_ffi::native_structured_schema_edit_add_bytes(
        **impl_->native, family.has_value(),
        detail::RustStr(FamilyValue(family)), column);
  });
  return *this;
}

SchemaBuilder &
SchemaBuilder::AddListColumn(std::optional<std::string_view> family,
                             ColumnIndex column, const ListConfig &config) {
  CheckBuilder(impl_.get());
  const auto native = detail::ToNative(config);
  detail::Translate([&] {
    structured_ffi::native_structured_schema_edit_add_list(
        **impl_->native, family.has_value(),
        detail::RustStr(FamilyValue(family)), column, native);
  });
  return *this;
}

SchemaBuilder &
SchemaBuilder::DeleteColumn(std::optional<std::string_view> family,
                            ColumnIndex column) {
  CheckBuilder(impl_.get());
  detail::Translate([&] {
    structured_ffi::native_structured_schema_edit_delete(
        **impl_->native, family.has_value(),
        detail::RustStr(FamilyValue(family)), column);
  });
  return *this;
}

SchemaBuilder &
SchemaBuilder::SetFamilyTtl(std::optional<std::string_view> family,
                            bool value_has_ttl) {
  CheckBuilder(impl_.get());
  detail::Translate([&] {
    structured_ffi::native_structured_schema_edit_set_family_ttl(
        **impl_->native, family.has_value(),
        detail::RustStr(FamilyValue(family)), value_has_ttl);
  });
  return *this;
}

Schema SchemaBuilder::Commit() {
  CheckBuilder(impl_.get());
  structured_ffi::NativeStructuredSchema native;
  if (auto *owner = std::get_if<std::shared_ptr<Db::Impl>>(&impl_->owner)) {
    native = detail::Translate([&] {
      return structured_ffi::native_structured_db_commit_schema(
          *(*owner)->native, **impl_->native);
    });
  } else {
    auto &single_owner =
        std::get<std::shared_ptr<SingleDb::Impl>>(impl_->owner);
    native = detail::Translate([&] {
      return structured_ffi::native_structured_single_db_commit_schema(
          *single_owner->native, **impl_->native);
    });
  }
  impl_->native.reset();
  impl_->owner = std::monostate{};
  impl_->committed = true;
  return detail::ToSchema(native);
}

} // namespace cobble::structured
