#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <cobble/snapshot.hpp>
#include <cobble/structured/scan.hpp>

namespace cobble::structured {

struct ScanSplitBoundary {
  BucketId bucket;
  std::vector<Byte> key;
};

struct ScanSplitPartition;

struct COBBLE_CPP_API ScanSplit {
  ShardSnapshot shard;
  std::optional<std::vector<Byte>> start_inclusive;
  std::optional<std::vector<Byte>> end_exclusive;
  std::optional<ScanSplitBoundary> start_after_exclusive;
  std::optional<ScanSplitBoundary> end_at_inclusive;

  [[nodiscard]] ScanSplitPartition SplitAfter(BucketId bucket,
                                              BytesView key_inclusive) const;
  [[nodiscard]] std::string ToJson() const;
  [[nodiscard]] static ScanSplit FromJson(std::string_view json);
  [[nodiscard]] ScanCursor OpenScanner(std::string_view config_json,
                                       const ScanOptions &options = {}) const;
  [[nodiscard]] ScanCursor
  OpenScannerFile(std::string_view config_path,
                  const ScanOptions &options = {}) const;
};

struct ScanSplitPartition {
  ScanSplit before;
  ScanSplit after;
};

class COBBLE_CPP_API ScanPlan final {
public:
  [[nodiscard]] static ScanPlan FromGlobalSnapshot(GlobalSnapshot snapshot);
  ScanPlan &WithStart(BytesView start_inclusive);
  ScanPlan &WithEnd(BytesView end_exclusive);
  ScanPlan &WithoutStart() noexcept;
  ScanPlan &WithoutEnd() noexcept;
  [[nodiscard]] std::vector<ScanSplit> Splits() const;

private:
  explicit ScanPlan(GlobalSnapshot snapshot);
  GlobalSnapshot snapshot_;
  std::optional<std::vector<Byte>> start_inclusive_;
  std::optional<std::vector<Byte>> end_exclusive_;
};

} // namespace cobble::structured
