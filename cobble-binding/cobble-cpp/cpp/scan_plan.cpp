#include <cobble/scan_plan.hpp>

#include <utility>

#include "detail/convert.hpp"
#include "detail/error.hpp"
#include "detail/impl.hpp"
#include "detail/options.hpp"

namespace cobble {

ScanPlan::ScanPlan(GlobalSnapshot snapshot) : snapshot_(std::move(snapshot)) {}

ScanPlan ScanPlan::FromGlobalSnapshot(GlobalSnapshot snapshot) {
  return ScanPlan(std::move(snapshot));
}

ScanPlan& ScanPlan::WithStart(BytesView start_inclusive) {
  start_inclusive_ =
      std::vector<Byte>(start_inclusive.begin(), start_inclusive.end());
  return *this;
}

ScanPlan& ScanPlan::WithEnd(BytesView end_exclusive) {
  end_exclusive_ =
      std::vector<Byte>(end_exclusive.begin(), end_exclusive.end());
  return *this;
}

ScanPlan& ScanPlan::WithoutStart() noexcept {
  start_inclusive_.reset();
  return *this;
}

ScanPlan& ScanPlan::WithoutEnd() noexcept {
  end_exclusive_.reset();
  return *this;
}

std::vector<ScanSplit> ScanPlan::Splits() const {
  std::vector<ScanSplit> splits;
  splits.reserve(snapshot_.shards.size());
  for (const auto& shard : snapshot_.shards) {
    splits.push_back(
        ScanSplit{shard, start_inclusive_, end_exclusive_, std::nullopt,
                  std::nullopt});
  }
  return splits;
}

ScanSplitPartition ScanSplit::SplitAfter(BucketId bucket,
                                         BytesView key_inclusive) const {
  auto native = detail::Translate([&] {
    return ffi::native_scan_split_split_after(
        detail::ToNativeScanSplit(*this), bucket,
        detail::RustBytes(key_inclusive));
  });
  if (native.size() != 2) {
    throw Error(ErrorCode::kInvalidState,
                "Rust bridge returned an invalid scan split partition");
  }
  return {detail::ToScanSplit(native[0]), detail::ToScanSplit(native[1])};
}

std::string ScanSplit::ToJson() const {
  const auto json = detail::Translate([&] {
    return ffi::native_scan_split_to_json(detail::ToNativeScanSplit(*this));
  });
  return {json.data(), json.size()};
}

ScanSplit ScanSplit::FromJson(std::string_view json) {
  return detail::ToScanSplit(detail::Translate(
      [&] { return ffi::native_scan_split_from_json(detail::RustStr(json)); }));
}

ScanCursor ScanSplit::OpenScanner(std::string_view config_json,
                                  const ScanOptions& options) const {
  const auto native_options = detail::ToNative(options);
  auto native = detail::Translate([&] {
    return ffi::native_scan_split_open_scanner(
        detail::RustStr(config_json), detail::ToNativeScanSplit(*this),
        native_options);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

ScanCursor ScanSplit::OpenScannerFile(std::string_view config_path,
                                      const ScanOptions& options) const {
  const auto native_options = detail::ToNative(options);
  auto native = detail::Translate([&] {
    return ffi::native_scan_split_open_scanner_file(
        detail::RustStr(config_path), detail::ToNativeScanSplit(*this),
        native_options);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

}  // namespace cobble
