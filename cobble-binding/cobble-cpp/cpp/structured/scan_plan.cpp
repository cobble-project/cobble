#include <cobble/structured/scan_plan.hpp>

#include <utility>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {
namespace {

rust::Vec<Byte> ToRustBytes(const std::vector<Byte> &value) {
  rust::Vec<Byte> result;
  result.reserve(value.size());
  for (const auto byte : value)
    result.push_back(byte);
  return result;
}

std::vector<Byte> ToBytes(const rust::Vec<Byte> &value) {
  return {value.begin(), value.end()};
}

std::vector<Byte> CopyBytes(BytesView value) {
  if (value.empty())
    return {};
  return {value.begin(), value.end()};
}

structured_ffi::NativeShardSnapshot ToNative(const ShardSnapshot &value) {
  structured_ffi::NativeShardSnapshot result;
  result.ranges.reserve(value.ranges.size());
  for (const auto &range : value.ranges) {
    structured_ffi::NativeBucketRange item;
    item.start_inclusive = range.start_inclusive;
    item.end_inclusive = range.end_inclusive;
    result.ranges.push_back(std::move(item));
  }
  result.families.reserve(value.column_families.size());
  for (const auto &family : value.column_families) {
    structured_ffi::NativeFamily item;
    item.name = rust::String(family.name);
    item.id = family.id;
    result.families.push_back(std::move(item));
  }
  result.db_id = rust::String(value.db_id);
  result.snapshot_id = value.snapshot_id;
  result.manifest_path = rust::String(value.manifest_path);
  result.timestamp_seconds = value.timestamp_seconds;
  result.data_size_bytes = value.data_size_bytes;
  result.incremental_data_size_bytes = value.incremental_data_size_bytes;
  return result;
}

structured_ffi::NativeStructuredScanSplit ToNative(const ScanSplit &value) {
  structured_ffi::NativeStructuredScanSplit result;
  result.shard = ToNative(value.shard);
  result.has_start = value.start_inclusive.has_value();
  result.start = value.start_inclusive ? ToRustBytes(*value.start_inclusive)
                                       : rust::Vec<Byte>{};
  result.has_end = value.end_exclusive.has_value();
  result.end = value.end_exclusive ? ToRustBytes(*value.end_exclusive)
                                   : rust::Vec<Byte>{};
  result.has_start_after = value.start_after_exclusive.has_value();
  result.start_after_bucket =
      value.start_after_exclusive ? value.start_after_exclusive->bucket : 0;
  result.start_after_key = value.start_after_exclusive
                               ? ToRustBytes(value.start_after_exclusive->key)
                               : rust::Vec<Byte>{};
  result.has_end_at = value.end_at_inclusive.has_value();
  result.end_at_bucket =
      value.end_at_inclusive ? value.end_at_inclusive->bucket : 0;
  result.end_at_key = value.end_at_inclusive
                          ? ToRustBytes(value.end_at_inclusive->key)
                          : rust::Vec<Byte>{};
  return result;
}

ScanSplit ToSplit(const structured_ffi::NativeStructuredScanSplit &value) {
  ScanSplit result;
  result.shard = detail::ToShardSnapshot(value.shard);
  if (value.has_start)
    result.start_inclusive = ToBytes(value.start);
  if (value.has_end)
    result.end_exclusive = ToBytes(value.end);
  if (value.has_start_after)
    result.start_after_exclusive = ScanSplitBoundary{
        value.start_after_bucket, ToBytes(value.start_after_key)};
  if (value.has_end_at)
    result.end_at_inclusive =
        ScanSplitBoundary{value.end_at_bucket, ToBytes(value.end_at_key)};
  return result;
}

} // namespace

ScanPlan::ScanPlan(GlobalSnapshot snapshot) : snapshot_(std::move(snapshot)) {}
ScanPlan ScanPlan::FromGlobalSnapshot(GlobalSnapshot snapshot) {
  return ScanPlan(std::move(snapshot));
}
ScanPlan &ScanPlan::WithStart(BytesView start_inclusive) {
  start_inclusive_ = CopyBytes(start_inclusive);
  return *this;
}
ScanPlan &ScanPlan::WithEnd(BytesView end_exclusive) {
  end_exclusive_ = CopyBytes(end_exclusive);
  return *this;
}
ScanPlan &ScanPlan::WithoutStart() noexcept {
  start_inclusive_.reset();
  return *this;
}
ScanPlan &ScanPlan::WithoutEnd() noexcept {
  end_exclusive_.reset();
  return *this;
}
std::vector<ScanSplit> ScanPlan::Splits() const {
  std::vector<ScanSplit> result;
  result.reserve(snapshot_.shards.size());
  for (const auto &shard : snapshot_.shards) {
    result.push_back(
        {shard, start_inclusive_, end_exclusive_, std::nullopt, std::nullopt});
  }
  return result;
}

ScanSplitPartition ScanSplit::SplitAfter(BucketId bucket,
                                         BytesView key_inclusive) const {
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_scan_split_split_after(
        ToNative(*this), bucket, detail::RustBytes(key_inclusive));
  });
  if (native.size() != 2)
    throw Error(ErrorCode::kInvalidState,
                "Rust bridge returned an invalid structured split partition");
  return {ToSplit(native[0]), ToSplit(native[1])};
}

std::string ScanSplit::ToJson() const {
  const auto json = detail::Translate([&] {
    return structured_ffi::native_structured_scan_split_to_json(
        ToNative(*this));
  });
  return {json.data(), json.size()};
}

ScanSplit ScanSplit::FromJson(std::string_view json) {
  return ToSplit(detail::Translate([&] {
    return structured_ffi::native_structured_scan_split_from_json(
        detail::RustStr(json));
  }));
}

ScanCursor ScanSplit::OpenScanner(std::string_view config_json,
                                  const ScanOptions &options) const {
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ScanOptions has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_scan_split_open_scanner(
        detail::RustStr(config_json), ToNative(*this), *options.impl_->native);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

ScanCursor ScanSplit::OpenScannerFile(std::string_view config_path,
                                      const ScanOptions &options) const {
  if (!options.impl_)
    throw Error(ErrorCode::kInvalidState, "ScanOptions has been moved from");
  auto native = detail::Translate([&] {
    return structured_ffi::native_structured_scan_split_open_scanner_file(
        detail::RustStr(config_path), ToNative(*this), *options.impl_->native);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

} // namespace cobble::structured
