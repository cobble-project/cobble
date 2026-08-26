#include <cobble/database.hpp>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"

namespace cobble {

std::uint32_t Database::NowSeconds() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return ffi::native_database_now_seconds(*impl_->native);
}

void Database::SwitchMemtableType(MemtableType type,
                                  bool flush_current) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  detail::Translate([&] {
    ffi::native_database_switch_memtable_type(
        *impl_->native, static_cast<std::uint8_t>(type), flush_current);
  });
}

std::size_t Database::LoadReadonlyFilesToPrimary() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return detail::ToSize(detail::Translate([&] {
                          return ffi::
                              native_database_load_readonly_files_to_primary(
                                  *impl_->native);
                        }),
                        "readonly file count");
}

}  // namespace cobble
