#include "test_support.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;
using cobble_test::String;

std::string Config(const std::filesystem::path& root) {
  return R"({"volumes":[{"base_dir":")" + FileUrl(root) +
         R"(","kinds":["meta","primary_data_priority_high","snapshot"]}],"num_columns":1,"total_buckets":4,"memtable_capacity":"8KB","base_file_size":"16KB","block_cache_size":0,"snapshot_retention":20,"wal_enabled":false})";
}

void WriteFile(const std::filesystem::path& path, std::string_view contents) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  COBBLE_CHECK(output.is_open());
  output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  output.close();
  COBBLE_CHECK(output.good());
}

void ExpectError(cobble::ErrorCode code, const auto& action) {
  bool rejected = false;
  try {
    action();
  } catch (const cobble::Error& error) {
    rejected = error.code() == code;
  }
  COBBLE_CHECK(rejected);
}

std::vector<std::pair<cobble::BucketId, std::string>> Collect(
    cobble::ScanCursor cursor) {
  std::vector<std::pair<cobble::BucketId, std::string>> rows;
  while (true) {
    auto batch = cursor.Next(2);
    for (std::size_t row = 0; row < batch.row_count(); ++row) {
      rows.emplace_back(batch.bucket(row), String(batch.key(row)));
    }
    if (batch.end()) {
      return rows;
    }
  }
}

struct SnapshotSet {
  cobble::ShardSnapshot left;
  cobble::ShardSnapshot right;
};

SnapshotSet TakeSnapshots(cobble::Db& left, cobble::Db& right) {
  return {left.TakeSnapshot(), right.TakeSnapshot()};
}

void VerifyCoverageValidation(const cobble::DbCoordinator& coordinator,
                              const SnapshotSet& snapshots) {
  const std::array valid = {snapshots.left, snapshots.right};
  ExpectError(cobble::ErrorCode::kInput, [&] {
    (void)coordinator.MaterializeGlobalSnapshot(0, 1, valid);
  });
  const std::vector<cobble::ShardSnapshot> empty;
  ExpectError(cobble::ErrorCode::kInput, [&] {
    (void)coordinator.MaterializeGlobalSnapshot(4, 1, empty);
  });

  auto gap = valid;
  gap[1].ranges = {{3, 3}};
  ExpectError(cobble::ErrorCode::kInput, [&] {
    (void)coordinator.MaterializeGlobalSnapshot(4, 1, gap);
  });

  auto overlap = valid;
  overlap[1].ranges = {{1, 3}};
  ExpectError(cobble::ErrorCode::kInput, [&] {
    (void)coordinator.MaterializeGlobalSnapshot(4, 1, overlap);
  });

  auto outside = valid;
  outside[1].ranges = {{2, 4}};
  ExpectError(cobble::ErrorCode::kInput, [&] {
    (void)coordinator.MaterializeGlobalSnapshot(4, 1, outside);
  });
}

void VerifyReadOnly(const std::string& config,
                    const std::filesystem::path& config_path,
                    const cobble::Db& source,
                    const cobble::ShardSnapshot& snapshot) {
  auto db = cobble::ReadOnlyDb::OpenFile(config_path.string(),
                                         snapshot.snapshot_id, source.Id());
  COBBLE_CHECK(db.Id() == source.Id());
  COBBLE_CHECK(String(db.Get(0, Bytes("version")).column(0)) == "old-left");

  cobble::ReadOptions one;
  one.columns = {0};
  std::array<cobble::Byte, 2> small{};
  const auto needed = db.GetColumnInto(0, Bytes("version"), small, one);
  COBBLE_CHECK(needed.status == cobble::BufferStatus::kBufferTooSmall);
  std::vector<cobble::Byte> output(needed.bytes_required);
  const auto copied = db.GetColumnInto(0, Bytes("version"), output, one);
  COBBLE_CHECK(copied.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(String(output) == "old-left");

  const std::array keys = {
      cobble::MultiGetKey{0, Bytes("version")},
      cobble::MultiGetKey{1, Bytes("b")},
      cobble::MultiGetKey{0, Bytes("version")},
      cobble::MultiGetKey{1, Bytes("missing")},
  };
  const auto rows = db.MultiGet(keys);
  COBBLE_CHECK(rows.row_count() == keys.size());
  COBBLE_CHECK(String(rows.column(0, 0)) == "old-left");
  COBBLE_CHECK(String(rows.column(1, 0)) == "left-1-b");
  COBBLE_CHECK(String(rows.column(2, 0)) == "old-left");
  COBBLE_CHECK(!rows.found(3));

  auto scan = db.Scan(1, std::nullopt, std::nullopt);
  std::array<cobble::Byte, 1> tiny{};
  const auto pending = scan.NextBatchInto(8, tiny);
  COBBLE_CHECK(pending.status == cobble::BufferStatus::kBufferTooSmall);
  std::vector<cobble::Byte> encoded(pending.bytes_required);
  const auto encoded_result = scan.NextBatchInto(8, encoded);
  COBBLE_CHECK(encoded_result.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(encoded_result.row_count == 2);

  COBBLE_CHECK(db.CurrentSchema().column_families.size() == 1);
  const auto metrics = db.Metrics();
  COBBLE_CHECK(std::any_of(metrics.begin(), metrics.end(),
                           [](const cobble::MetricSample& sample) {
                             return !sample.name.empty();
                           }));

  auto surviving = [&] {
    auto temporary = cobble::ReadOnlyDb::Open(
        config, snapshot.snapshot_id, source.Id());
    return temporary.Scan(0, std::nullopt, std::nullopt);
  }();
  COBBLE_CHECK(!Collect(std::move(surviving)).empty());
}

void VerifyPlanAndSplit(const std::string& config,
                        const std::filesystem::path& config_path,
                        const cobble::GlobalSnapshot& global) {
  const std::array<cobble::Byte, 2> binary_start = {0x80, 0x00};
  const std::array<cobble::Byte, 2> binary_end = {0xFF, 0x7F};
  auto binary_plan = cobble::ScanPlan::FromGlobalSnapshot(global);
  binary_plan.WithStart(binary_start).WithEnd(binary_end);
  const auto binary_splits = binary_plan.Splits();
  COBBLE_CHECK(binary_splits.size() == 2);
  COBBLE_CHECK(*binary_splits[0].start_inclusive ==
               std::vector<cobble::Byte>(binary_start.begin(),
                                         binary_start.end()));
  COBBLE_CHECK(*binary_splits[0].end_exclusive ==
               std::vector<cobble::Byte>(binary_end.begin(), binary_end.end()));

  auto plan = cobble::ScanPlan::FromGlobalSnapshot(global);
  plan.WithStart(Bytes("a")).WithEnd(Bytes("z"));
  auto splits = plan.Splits();
  COBBLE_CHECK(splits.size() == 2);

  auto encoded = splits[0];
  encoded.start_inclusive = std::vector<cobble::Byte>{0xFC, 0x01};
  encoded.end_exclusive = std::vector<cobble::Byte>{0xFE, 0x02};
  encoded.start_after_exclusive =
      cobble::ScanSplitBoundary{0, {0xFF, 0x05}};
  encoded.end_at_inclusive = cobble::ScanSplitBoundary{1, {0xF8, 0x07}};
  const auto encoded_json = encoded.ToJson();
  const auto rebound = cobble::ScanSplit::FromJson(encoded_json);
  COBBLE_CHECK(rebound.start_inclusive == encoded.start_inclusive);
  COBBLE_CHECK(rebound.end_exclusive == encoded.end_exclusive);
  COBBLE_CHECK(rebound.start_after_exclusive->key ==
               encoded.start_after_exclusive->key);
  COBBLE_CHECK(rebound.end_at_inclusive->key == encoded.end_at_inclusive->key);

  auto malformed_json = encoded_json;
  const auto start_bucket = malformed_json.find("\"start_bucket\":0");
  COBBLE_CHECK(start_bucket != std::string::npos);
  malformed_json.replace(
      start_bucket, std::string_view("\"start_bucket\":0").size(),
      "\"start_bucket\":null");
  ExpectError(cobble::ErrorCode::kInput, [&] {
    (void)cobble::ScanSplit::FromJson(malformed_json);
  });

  const auto all = Collect(splits[0].OpenScanner(config));
  COBBLE_CHECK((all == std::vector<std::pair<cobble::BucketId, std::string>>{
                           {0, "a"}, {0, "b"}, {0, "version"},
                           {1, "a"}, {1, "b"}}));

  const auto partition = splits[0].SplitAfter(0, Bytes("a"));
  auto before = Collect(partition.before.OpenScannerFile(config_path.string()));
  auto after = Collect(partition.after.OpenScannerFile(config_path.string()));
  before.insert(before.end(), after.begin(), after.end());
  COBBLE_CHECK(before == all);

  auto multi_range = splits[0];
  multi_range.shard.ranges = {{0, 0}, {1, 1}};
  ExpectError(cobble::ErrorCode::kInput, [&] {
    (void)multi_range.SplitAfter(0, Bytes("a"));
  });

  cobble::ScanOptions unsupported;
  unsupported.stop_at_block_boundary = true;
  ExpectError(cobble::ErrorCode::kInput, [&] {
    (void)splits[0].OpenScanner(config, unsupported);
  });

  auto caller_buffer = splits[1].OpenScanner(config);
  std::array<cobble::Byte, 1> tiny{};
  const auto pending = caller_buffer.NextBatchInto(10, tiny);
  COBBLE_CHECK(pending.status == cobble::BufferStatus::kBufferTooSmall);
  std::vector<cobble::Byte> output(pending.bytes_required);
  const auto copied = caller_buffer.NextBatchInto(10, output);
  COBBLE_CHECK(copied.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(copied.row_count == 5);
}

void VerifyReaders(const std::string& config,
                   const std::filesystem::path& config_path,
                   cobble::DbCoordinator& coordinator, cobble::Db& left,
                   cobble::Db& right, const cobble::GlobalSnapshot& first) {
  auto pinned = cobble::Reader::OpenFile(config_path.string(), first.id);
  auto current = cobble::Reader::OpenCurrent(config);
  COBBLE_CHECK(pinned.Mode() == cobble::ReaderMode::kSnapshot);
  COBBLE_CHECK(pinned.ConfiguredSnapshotId() == first.id);
  COBBLE_CHECK(current.Mode() == cobble::ReaderMode::kCurrent);
  COBBLE_CHECK(!current.ConfiguredSnapshotId().has_value());
  COBBLE_CHECK(current.CurrentGlobalSnapshot().id == first.id);
  COBBLE_CHECK(!current.ListGlobalSnapshots().empty());

  const std::array keys = {
      cobble::MultiGetKey{0, Bytes("version")},
      cobble::MultiGetKey{2, Bytes("version")},
      cobble::MultiGetKey{0, Bytes("version")},
      cobble::MultiGetKey{3, Bytes("missing")},
  };
  const auto rows = current.MultiGet(keys);
  COBBLE_CHECK(String(rows.column(0, 0)) == "old-left");
  COBBLE_CHECK(String(rows.column(1, 0)) == "old-right");
  COBBLE_CHECK(String(rows.column(2, 0)) == "old-left");
  COBBLE_CHECK(!rows.found(3));

  cobble::ReadOptions one;
  one.columns = {0};
  std::array<cobble::Byte, 2> small{};
  const auto needed = current.GetColumnInto(2, Bytes("version"), small, one);
  COBBLE_CHECK(needed.status == cobble::BufferStatus::kBufferTooSmall);
  std::vector<cobble::Byte> output(needed.bytes_required);
  COBBLE_CHECK(current.GetColumnInto(2, Bytes("version"), output, one).status ==
               cobble::BufferStatus::kOk);
  COBBLE_CHECK(String(output) == "old-right");

  auto surviving = [&] {
    auto temporary = cobble::Reader::Open(config, first.id);
    return temporary.Scan(0, Bytes("a"), Bytes("z"));
  }();
  COBBLE_CHECK(!Collect(std::move(surviving)).empty());

  left.Put(0, Bytes("version"), 0, Bytes("new-left"));
  right.Put(2, Bytes("version"), 0, Bytes("new-right"));
  const auto snapshots = TakeSnapshots(left, right);
  const std::array shards = {snapshots.left, snapshots.right};
  const auto second =
      coordinator.MaterializeGlobalSnapshot(4, first.id + 1, shards);

  ExpectError(cobble::ErrorCode::kInvalidState,
              [&] { pinned.Refresh(); });
  COBBLE_CHECK(String(pinned.Get(0, Bytes("version")).column(0)) ==
               "old-left");
  current.Refresh();
  COBBLE_CHECK(current.CurrentGlobalSnapshot().id == second.id);
  COBBLE_CHECK(String(current.Get(0, Bytes("version")).column(0)) ==
               "new-left");
  COBBLE_CHECK(String(current.Get(2, Bytes("version")).column(0)) ==
               "new-right");
}

}  // namespace

int main() {
  try {
    cobble_test::TempDirectory directory("cobble-cpp-read-surface");
    const auto config = Config(directory.path() / "database");
    const auto config_path = directory.path() / "config.json";
    WriteFile(config_path, config);

    const std::array left_range = {cobble::BucketRange{0, 1}};
    const std::array right_range = {cobble::BucketRange{2, 3}};
    auto left = cobble::Db::Open(config, left_range);
    auto right = cobble::Db::Open(config, right_range);
    left.Put(0, Bytes("a"), 0, Bytes("left-0-a"));
    left.Put(0, Bytes("b"), 0, Bytes("left-0-b"));
    left.Put(0, Bytes("version"), 0, Bytes("old-left"));
    left.Put(1, Bytes("a"), 0, Bytes("left-1-a"));
    left.Put(1, Bytes("b"), 0, Bytes("left-1-b"));
    right.Put(2, Bytes("a"), 0, Bytes("right-2-a"));
    right.Put(2, Bytes("b"), 0, Bytes("right-2-b"));
    right.Put(2, Bytes("version"), 0, Bytes("old-right"));
    right.Put(3, Bytes("a"), 0, Bytes("right-3-a"));
    right.Put(3, Bytes("b"), 0, Bytes("right-3-b"));

    const auto snapshots = TakeSnapshots(left, right);
    auto coordinator = cobble::DbCoordinator::OpenFile(config_path.string());
    VerifyCoverageValidation(coordinator, snapshots);
    const std::array shards = {snapshots.left, snapshots.right};
    const auto first = coordinator.MaterializeGlobalSnapshot(4, 100, shards);
    COBBLE_CHECK(coordinator.GetGlobalSnapshot(first.id).id == first.id);
    COBBLE_CHECK(coordinator.LoadCurrentGlobalSnapshot()->id == first.id);
    COBBLE_CHECK(coordinator.RetainSnapshot(first.id));

    VerifyReadOnly(config, config_path, left, snapshots.left);
    VerifyPlanAndSplit(config, config_path, first);
    VerifyReaders(config, config_path, coordinator, left, right, first);

    COBBLE_CHECK(coordinator.ExpireSnapshot(first.id));
    COBBLE_CHECK(coordinator.ListGlobalSnapshots().size() == 1);
    left.Close();
    right.Close();
    std::cout << "verified C++ Reader, ReadOnlyDb, coordinator, and scan splits\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "read surface capability test failed: " << error.what()
              << '\n';
    return 1;
  }
}
