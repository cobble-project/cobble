#include "test_support.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;
using cobble_test::String;

constexpr std::size_t kBoundaryRows = 1'024;
constexpr std::size_t kBoundaryValueBytes = 512;

std::string ScanKey(std::size_t row) {
  std::ostringstream stream;
  stream << "scan-" << std::setw(6) << std::setfill('0') << row;
  return stream.str();
}

std::string ScanValue(std::size_t row) {
  std::string value = "row=" + std::to_string(row) + ";";
  value.resize(kBoundaryValueBytes, static_cast<char>('a' + (row % 26)));
  return value;
}

void WriteConfigFile(const std::filesystem::path& path,
                     std::string_view config) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  COBBLE_CHECK(output.is_open());
  output.write(config.data(), static_cast<std::streamsize>(config.size()));
  output.close();
  COBBLE_CHECK(output.good());
}

cobble::SnapshotId WaitForSnapshot(const cobble::Database& db,
                                   cobble::SnapshotId snapshot) {
  for (std::size_t attempt = 0; attempt < 3'000; ++attempt) {
    const auto snapshots = db.ListSnapshots();
    if (std::find(snapshots.begin(), snapshots.end(), snapshot) !=
        snapshots.end()) {
      const auto manifest = db.SnapshotManifestJson(snapshot);
      COBBLE_CHECK(manifest.find("\"id\":") != std::string::npos);
      COBBLE_CHECK(manifest.find(std::to_string(snapshot)) !=
                   std::string::npos);
      return snapshot;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  throw std::runtime_error("snapshot did not materialize within 30 seconds");
}

void WriteBoundaryData(const cobble::Database& db) {
  cobble::WriteOptions options;
  options.column_family = "default";
  options.await_durable = false;

  cobble::WriteBatch batch;
  for (std::size_t row = 0; row < kBoundaryRows; ++row) {
    const auto key = ScanKey(row);
    const auto value = ScanValue(row);
    batch.Put(0, Bytes(key), 0, Bytes(value), options);
    if (batch.size() == 64) {
      db.Write(std::move(batch), false);
      batch = cobble::WriteBatch();
    }
  }
  COBBLE_CHECK(batch.size() == 0);
}

void VerifyPointAndMutationApis(cobble::Database& db) {
  cobble::WriteOptions default_family;
  default_family.column_family = "default";
  default_family.await_durable = true;

  db.Put(1, Bytes("direct"), 0, Bytes("column-0"), default_family);
  db.Put(1, Bytes("direct"), 1, Bytes("old"), default_family);
  db.Put(1, Bytes("direct"), 2, Bytes("column-2"), default_family);
  db.Merge(1, Bytes("direct"), 1, Bytes("-merged"), default_family);

  auto direct = db.Get(1, Bytes("direct"));
  COBBLE_CHECK(direct.found());
  COBBLE_CHECK(direct.column_count() == 3);
  COBBLE_CHECK(direct.has_column(0));
  COBBLE_CHECK(String(direct.column(0)) == "column-0");
  COBBLE_CHECK(String(direct.column(1)) == "old-merged");
  COBBLE_CHECK(String(direct.column(2)) == "column-2");

  cobble::ReadOptions projected;
  projected.column_family = "default";
  projected.columns = {2, 0};
  auto projection = db.Get(1, Bytes("direct"), projected);
  COBBLE_CHECK(projection.found());
  COBBLE_CHECK(projection.column_count() == 2);
  COBBLE_CHECK(String(projection.column(0)) == "column-2");
  COBBLE_CHECK(String(projection.column(1)) == "column-0");

  cobble::ReadOptions one_column;
  one_column.column_family = "default";
  one_column.columns = {1};
  std::array<std::uint8_t, 2> small = {0xA5, 0x5A};
  const auto too_small =
      db.GetColumnInto(1, Bytes("direct"), small, one_column);
  COBBLE_CHECK(too_small.status == cobble::BufferStatus::kBufferTooSmall);
  COBBLE_CHECK(too_small.bytes_required == 10);
  COBBLE_CHECK((small == std::array<std::uint8_t, 2>{0xA5, 0x5A}));

  std::vector<std::uint8_t> output(too_small.bytes_required);
  const auto copied =
      db.GetColumnInto(1, Bytes("direct"), output, one_column);
  COBBLE_CHECK(copied.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(copied.bytes_written == output.size());
  COBBLE_CHECK(String(output) == "old-merged");
  const auto missing =
      db.GetColumnInto(1, Bytes("missing"), output, one_column);
  COBBLE_CHECK(missing.status == cobble::BufferStatus::kNotFound);
  COBBLE_CHECK(missing.bytes_written == 0);

  db.Put(1, Bytes("delete-direct"), 0, Bytes("value"), default_family);
  db.Delete(1, Bytes("delete-direct"), 0, default_family);
  COBBLE_CHECK(!db.Get(1, Bytes("delete-direct")).found());

  db.Put(1, Bytes("delete-batch"), 0, Bytes("value"), default_family);
  cobble::WriteBatch batch;
  batch.Put(1, Bytes("batch"), 0, Bytes("base"), default_family);
  batch.Merge(1, Bytes("batch"), 0, Bytes("-merged"), default_family);
  batch.Put(1, Bytes("batch"), 2, Bytes("third"), default_family);
  batch.Delete(1, Bytes("delete-batch"), 0, default_family);
  COBBLE_CHECK(batch.size() == 4);
  db.Write(std::move(batch), false);

  auto batch_row = db.Get(1, Bytes("batch"));
  COBBLE_CHECK(batch_row.found());
  COBBLE_CHECK(String(batch_row.column(0)) == "base-merged");
  COBBLE_CHECK(!batch_row.has_column(1));
  COBBLE_CHECK(String(batch_row.column(2)) == "third");
  COBBLE_CHECK(!db.Get(1, Bytes("delete-batch")).found());

  cobble::ScanOptions scan_options;
  scan_options.column_family = "default";
  scan_options.columns = {2, 0};
  scan_options.read_ahead_bytes = 4'096;
  scan_options.max_rows = 64;
  scan_options.preload_scan_cursor_block = true;
  auto scan = db.Scan(1, Bytes("batch"), Bytes("batci"), scan_options);
  const auto scan_batch = scan.Next(16);
  COBBLE_CHECK(scan_batch.row_count() == 1);
  COBBLE_CHECK(scan_batch.bucket(0) == 1);
  COBBLE_CHECK(String(scan_batch.key(0)) == "batch");
  COBBLE_CHECK(scan_batch.column_count(0) == 2);
  COBBLE_CHECK(String(scan_batch.column(0, 0)) == "third");
  COBBLE_CHECK(String(scan_batch.column(0, 1)) == "base-merged");

  db.SetTime(1'000);
  cobble::WriteOptions ttl;
  ttl.column_family = "default";
  ttl.ttl_seconds = 1;
  ttl.await_durable = true;
  db.Put(1, Bytes("ttl"), 0, Bytes("expires"), ttl);
  COBBLE_CHECK(db.Get(1, Bytes("ttl")).found());
  db.SetTime(1'002);
  COBBLE_CHECK(!db.Get(1, Bytes("ttl")).found());
}

void VerifyBlockBoundaryScan(const cobble::Database& db) {
  {
    cobble::ScanOptions ordered_options;
    ordered_options.column_family = "default";
    ordered_options.columns = {0};
    auto ordered =
        db.Scan(0, Bytes("scan-"), Bytes("scan."), ordered_options);
    std::size_t expected = 0;
    while (true) {
      const auto batch = ordered.Next(73);
      for (std::size_t row = 0; row < batch.row_count(); ++row) {
        COBBLE_CHECK(String(batch.key(row)) == ScanKey(expected));
        ++expected;
      }
      if (batch.end()) {
        break;
      }
    }
    COBBLE_CHECK(expected == kBoundaryRows);
  }

  cobble::ScanOptions options;
  options.column_family = "default";
  options.columns = {0};
  options.read_ahead_bytes = 8'192;
  options.preload_scan_cursor_block = true;
  options.stop_at_block_boundary = true;

  auto scan = db.Scan(0, Bytes("scan-"), Bytes("scan."), options);
  std::size_t expected_row = 0;
  std::size_t boundaries = 0;
  bool end = false;
  while (!end) {
    const auto batch = scan.Next(73);
    for (std::size_t row = 0; row < batch.row_count(); ++row) {
      COBBLE_CHECK(expected_row < kBoundaryRows);
      COBBLE_CHECK(batch.bucket(row) == 0);
      const auto actual_key = String(batch.key(row));
      const auto expected_key = ScanKey(expected_row);
      if (actual_key != expected_key) {
        throw std::runtime_error("boundary scan expected " + expected_key +
                                 " but read " + actual_key);
      }
      COBBLE_CHECK(batch.column_count(row) == 1);
      COBBLE_CHECK(String(batch.column(row, 0)) == ScanValue(expected_row));
      ++expected_row;
    }
    if (batch.stopped_at_block_boundary()) {
      ++boundaries;
      scan.ResumeAfterBlockBoundary();
    }
    end = batch.end();
  }
  COBBLE_CHECK(expected_row == kBoundaryRows);
  COBBLE_CHECK(boundaries > 0);
}

void VerifyClosedHandle(cobble::Database& db) {
  bool rejected = false;
  try {
    (void)db.Get(0, Bytes("closed"));
  } catch (const cobble::Error& error) {
    rejected = error.code() == cobble::ErrorCode::kInvalidState;
  }
  COBBLE_CHECK(rejected);
}

}  // namespace

int main() {
  try {
    cobble_test::TempDirectory directory("cobble-cpp-api-coverage");
    const auto data_url = FileUrl(directory.path() / "database");
    const std::string config =
        R"({"volumes":[{"base_dir":")" + data_url +
        R"(","kinds":["meta","primary_data_priority_high","snapshot"]},{"base_dir":")" +
        data_url +
        R"(","kinds":["wal"]}],"num_columns":3,"total_buckets":4,"memtable_capacity":"16KB","base_file_size":"32KB","block_cache_size":0,"ttl_enabled":true,"time_provider":"manual","wal_enabled":true,"wal_flush_interval_ms":5})";
    const auto config_path = directory.path() / "config.json";
    WriteConfigFile(config_path, config);

    cobble::SnapshotId first_snapshot = 0;
    {
      auto db = cobble::Database::OpenFile(config_path.string());
      COBBLE_CHECK(!cobble::Version().empty());
      VerifyPointAndMutationApis(db);
      WriteBoundaryData(db);
      first_snapshot = WaitForSnapshot(db, db.Snapshot());
      COBBLE_CHECK(db.RetainSnapshot(first_snapshot));
      db.Put(2, Bytes("wal-tail"), 0, Bytes("after-snapshot"));
      db.Close();
      VerifyClosedHandle(db);
    }

    {
      auto wal_recovered = cobble::Database::ResumeFile(
          config_path.string(), first_snapshot,
          cobble::RecoveryMode::kLatestWithWal);
      COBBLE_CHECK(wal_recovered.Get(2, Bytes("wal-tail")).found());
      COBBLE_CHECK(String(wal_recovered.Get(2, Bytes("wal-tail")).column(0)) ==
                   "after-snapshot");
      VerifyBlockBoundaryScan(wal_recovered);
      wal_recovered.Close();
    }

    cobble::SnapshotId second_snapshot = 0;
    {
      auto snapshot_only = cobble::Database::Resume(
          config, first_snapshot, cobble::RecoveryMode::kSnapshotOnly);
      COBBLE_CHECK(!snapshot_only.Get(2, Bytes("wal-tail")).found());
      COBBLE_CHECK(snapshot_only.Get(1, Bytes("direct")).found());
      snapshot_only.Put(3, Bytes("after-resume"), 0, Bytes("snapshot-2"));
      second_snapshot = WaitForSnapshot(snapshot_only, snapshot_only.Snapshot());
      COBBLE_CHECK(second_snapshot > first_snapshot);

      const auto snapshots = snapshot_only.ListSnapshots();
      COBBLE_CHECK(std::find(snapshots.begin(), snapshots.end(),
                             first_snapshot) != snapshots.end());
      COBBLE_CHECK(std::find(snapshots.begin(), snapshots.end(),
                             second_snapshot) != snapshots.end());
      COBBLE_CHECK(snapshot_only.RetainSnapshot(second_snapshot));
      const bool first_expired =
          snapshot_only.ExpireSnapshot(first_snapshot);
      const auto after_expire = snapshot_only.ListSnapshots();
      const bool first_is_absent =
          std::find(after_expire.begin(), after_expire.end(), first_snapshot) ==
          after_expire.end();
      COBBLE_CHECK(first_is_absent == first_expired);
      snapshot_only.Close();
    }

    {
      auto resumed =
          cobble::Database::ResumeFile(config_path.string(), second_snapshot);
      const auto row = resumed.Get(3, Bytes("after-resume"));
      COBBLE_CHECK(row.found());
      COBBLE_CHECK(String(row.column(0)) == "snapshot-2");
      COBBLE_CHECK(!resumed.Get(2, Bytes("wal-tail")).found());
      resumed.Close();
    }

    std::cout << "verified complete C++ API, snapshot lifecycle, SnapshotOnly, "
                 "and LatestWithWal recovery\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "C++ API coverage test failed: " << error.what() << '\n';
    return 1;
  }
}
