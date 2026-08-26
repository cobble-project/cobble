#include "test_support.hpp"

#include <cobble/structured.hpp>

#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;
using cobble_test::String;

std::string Config(const std::filesystem::path &root) {
  const auto url = FileUrl(root);
  return R"({"volumes":[{"base_dir":")" + url +
         R"(","kinds":["meta","primary_data_priority_high","snapshot"]},{"base_dir":")" +
         url +
         R"(","kinds":["wal"]}],"num_columns":1,"total_buckets":4,"memtable_capacity":"8KB","base_file_size":"16KB","block_size":"1KB","block_cache_size":0,"wal_enabled":true,"wal_flush_interval_ms":5})";
}

std::uint32_t ReadU32(cobble::BytesView bytes, std::size_t offset) {
  COBBLE_CHECK(offset + 4 <= bytes.size());
  return static_cast<std::uint32_t>(bytes[offset]) |
         static_cast<std::uint32_t>(bytes[offset + 1]) << 8 |
         static_cast<std::uint32_t>(bytes[offset + 2]) << 16 |
         static_cast<std::uint32_t>(bytes[offset + 3]) << 24;
}

void CheckCsrb(const std::vector<cobble::Byte> &buffer,
               const cobble::BufferResult &result, std::size_t expected_rows) {
  COBBLE_CHECK(result.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(result.bytes_written == result.bytes_required);
  COBBLE_CHECK(result.row_count == expected_rows);
  COBBLE_CHECK(result.bytes_written >= 24);
  const cobble::BytesView bytes(buffer.data(), result.bytes_written);
  COBBLE_CHECK(std::memcmp(bytes.data(), "CSRB", 4) == 0);
  COBBLE_CHECK(bytes[4] == 1 && bytes[5] == 0);
  COBBLE_CHECK(ReadU32(bytes, 12) == expected_rows);
}

template <typename Database> void AddSchema(Database &db) {
  auto edit = db.UpdateSchema();
  edit.AddListColumn(std::nullopt, 1,
                     cobble::structured::ListConfig{.max_elements = 8});
  (void)edit.Commit();
}

template <typename Database> void VerifyBatchAndMultiGet(Database &db) {
  AddSchema(db);

  cobble::structured::WriteBatch owned;
  {
    std::string key = "owned-key";
    std::string value = "owned-value";
    std::vector<std::string> elements = {"owned-a", "owned-b"};
    std::array<cobble::BytesView, 2> views = {Bytes(elements[0]),
                                              Bytes(elements[1])};
    owned.PutBytes(0, Bytes(key), 0, Bytes(value));
    owned.PutList(0, Bytes(key), 1, views);
  }
  db.Write(owned);
  COBBLE_CHECK(owned.empty());
  auto retained = db.Get(0, Bytes("owned-key"));
  COBBLE_CHECK(String(retained.Bytes(0)) == "owned-value");
  COBBLE_CHECK(String(retained.ListElement(1, 1)) == "owned-b");

  constexpr std::size_t kRows = 320;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  std::vector<cobble::structured::WriteOperation> operations;
  keys.reserve(kRows);
  values.reserve(kRows);
  operations.reserve(kRows);
  for (std::size_t index = 0; index < kRows; ++index) {
    keys.push_back("key-" + std::to_string(index));
    values.push_back(std::string(192, static_cast<char>('a' + index % 20)));
    operations.push_back(cobble::structured::WriteOperation::PutBytes(
        static_cast<cobble::BucketId>(index % 4), Bytes(keys.back()), 0,
        Bytes(values.back())));
  }
  db.Write(operations); // synchronous borrowed high-throughput path

  const std::array multi = {
      cobble::structured::MultiGetKey{0, Bytes("key-0")},
      cobble::structured::MultiGetKey{3, Bytes("key-3")},
      cobble::structured::MultiGetKey{0, Bytes("key-0")},
      cobble::structured::MultiGetKey{2, Bytes("missing")},
      cobble::structured::MultiGetKey{1, cobble::BytesView{}},
  };
  auto rows = db.MultiGet(multi);
  COBBLE_CHECK(rows.RowCount() == multi.size());
  COBBLE_CHECK(rows.Found(0) && rows.Found(1) && rows.Found(2));
  COBBLE_CHECK(!rows.Found(3) && !rows.Found(4));
  COBBLE_CHECK(String(rows.Bytes(0, 0)) == values[0]);
  const auto lifetime = rows.Bytes(1, 0);
  keys.clear();
  values.clear();
  COBBLE_CHECK(lifetime.size() == 192);

  cobble::structured::ReadOptions projected;
  const std::array<std::size_t, 1> list_only = {1};
  projected.SetColumns(list_only);
  const std::array projected_keys = {
      cobble::structured::MultiGetKey{0, Bytes("owned-key")}};
  auto projected_rows = db.MultiGet(projected_keys, projected);
  COBBLE_CHECK(projected_rows.ColumnCount(0) == 1);
  COBBLE_CHECK(projected_rows.Kind(0, 0) ==
               cobble::structured::ColumnKind::kList);

  std::array<cobble::Byte, 8> small;
  small.fill(0xa5);
  const auto before = small;
  const auto small_result = db.MultiGetInto(multi, small);
  COBBLE_CHECK(small_result.status == cobble::BufferStatus::kBufferTooSmall);
  COBBLE_CHECK(small_result.bytes_written == 0);
  COBBLE_CHECK(small == before);
  std::vector<cobble::Byte> encoded(small_result.bytes_required);
  CheckCsrb(encoded, db.MultiGetInto(multi, encoded), multi.size());

  std::array<cobble::Byte, 1> point_small = {0x7f};
  const auto point_before = point_small;
  const auto point_size = db.GetInto(0, Bytes("owned-key"), point_small);
  COBBLE_CHECK(point_size.status == cobble::BufferStatus::kBufferTooSmall);
  COBBLE_CHECK(point_small == point_before);
  std::vector<cobble::Byte> point(point_size.bytes_required);
  CheckCsrb(point, db.GetInto(0, Bytes("owned-key"), point), 1);
  std::array<cobble::Byte, 128> missing_point{};
  const auto missing_result =
      db.GetInto(3, Bytes("point-missing"), missing_point);
  COBBLE_CHECK(missing_result.status == cobble::BufferStatus::kNotFound);
  COBBLE_CHECK(missing_result.row_count == 1);

  const std::array invalid = {
      cobble::structured::WriteOperation::PutBytes(0, Bytes("atomic-good"), 0,
                                                   Bytes("not-written")),
      cobble::structured::WriteOperation::PutBytes(0, Bytes("atomic-bad"), 1,
                                                   Bytes("wrong-type")),
  };
  bool rejected = false;
  try {
    db.Write(invalid);
  } catch (const cobble::Error &error) {
    rejected = error.code() == cobble::ErrorCode::kInput;
  }
  COBBLE_CHECK(rejected);
  COBBLE_CHECK(!db.Get(0, Bytes("atomic-good")).Found());

  cobble::structured::WriteBatch empty_payloads;
  {
    std::string temporary_empty;
    const std::array<cobble::BytesView, 1> empty_element = {
        Bytes(temporary_empty)};
    empty_payloads.PutBytes(2, cobble::BytesView{}, 0, cobble::BytesView{});
    empty_payloads.PutList(2, cobble::BytesView{}, 1, empty_element);
  }
  db.Write(empty_payloads);
  auto empty_row = db.Get(2, cobble::BytesView{});
  COBBLE_CHECK(empty_row.Found());
  COBBLE_CHECK(empty_row.Bytes(0).empty());
  COBBLE_CHECK(empty_row.ListSize(1) == 1);
  COBBLE_CHECK(empty_row.ListElement(1, 0).empty());
}

template <typename Database> void VerifyScan(Database &db) {
  cobble::structured::ScanOptions projected;
  const std::array<std::size_t, 1> columns = {0};
  projected.SetColumns(columns);
  auto cursor = db.Scan(0, Bytes("key-"), Bytes("key."), projected);
  std::set<std::string> seen;
  for (;;) {
    auto batch = cursor.Next(17);
    for (std::size_t row = 0; row < batch.RowCount(); ++row) {
      COBBLE_CHECK(batch.Bucket(row) == 0);
      COBBLE_CHECK(batch.ColumnCount(row) == 1);
      seen.insert(String(batch.Key(row)));
    }
    if (batch.End())
      break;
    COBBLE_CHECK(!batch.StoppedAtBlockBoundary());
  }
  COBBLE_CHECK(seen.size() == 80);

  auto into_cursor = db.Scan(0);
  std::array<cobble::Byte, 4> small = {1, 2, 3, 4};
  const auto before = small;
  const auto needed = into_cursor.NextBatchInto(23, small);
  COBBLE_CHECK(needed.status == cobble::BufferStatus::kBufferTooSmall);
  COBBLE_CHECK(small == before);
  std::vector<cobble::Byte> output(needed.bytes_required);
  CheckCsrb(output, into_cursor.NextBatchInto(23, output), 23);
  auto empty_cursor = db.Scan(3, Bytes("zzz"), Bytes("zzzz"));
  std::array<cobble::Byte, 64> empty_output{};
  const auto empty_result = empty_cursor.NextBatchInto(4, empty_output);
  COBBLE_CHECK(empty_result.status == cobble::BufferStatus::kEnd);
  COBBLE_CHECK(empty_result.row_count == 0);

  cobble::structured::ScanOptions boundary;
  boundary.SetStopAtBlockBoundary(true);
  auto boundary_cursor = db.Scan(0, std::nullopt, std::nullopt, boundary);
  std::set<std::string> boundary_seen;
  for (;;) {
    auto batch = boundary_cursor.Next(19);
    for (std::size_t row = 0; row < batch.RowCount(); ++row)
      COBBLE_CHECK(boundary_seen.insert(String(batch.Key(row))).second);
    if (batch.End())
      break;
    if (batch.StoppedAtBlockBoundary())
      boundary_cursor.ResumeAfterBlockBoundary();
  }
  COBBLE_CHECK(boundary_seen.size() >= 80);
}

void VerifySharded(const std::filesystem::path &root) {
  auto db = cobble::structured::Db::Open(Config(root));
  VerifyBatchAndMultiGet(db);
  auto retry_builder = db.UpdateSchema();
  retry_builder.AddBytesColumn(std::nullopt, 2);
  {
    auto cursor = db.Scan(0);
    bool rejected = false;
    try {
      (void)retry_builder.Commit();
    } catch (const cobble::Error &error) {
      rejected = error.code() == cobble::ErrorCode::kInvalidState;
    }
    COBBLE_CHECK(rejected);
  }
  const auto retried_schema = retry_builder.Commit();
  COBBLE_CHECK(retried_schema.Type("default", 2).kind ==
               cobble::structured::ColumnKind::kBytes);
  bool double_commit_rejected = false;
  try {
    (void)retry_builder.Commit();
  } catch (const cobble::Error &error) {
    double_commit_rejected = error.code() == cobble::ErrorCode::kInvalidState;
  }
  COBBLE_CHECK(double_commit_rejected);
  const auto snapshot = db.TakeSnapshot();
  {
    auto cursor = db.Scan(0);
    bool rejected = false;
    try {
      db.SwitchToSnapshot(snapshot.snapshot_id);
    } catch (const cobble::Error &error) {
      rejected = error.code() == cobble::ErrorCode::kInvalidState;
    }
    COBBLE_CHECK(rejected);
    (void)cursor.Next(1);
  }
  db.SwitchToSnapshot(snapshot.snapshot_id);
  VerifyScan(db);

  auto detached = db.Scan(0);
  db = cobble::structured::Db::Open(Config(root / "replacement"));
  COBBLE_CHECK(detached.Next(1).RowCount() == 1);
  db.Close();
}

void VerifySingleAndPlan(const std::filesystem::path &root) {
  std::filesystem::create_directories(root);
  const auto config = Config(root);
  const auto config_path = root / "config.json";
  {
    std::ofstream output(config_path);
    output << config;
  }
  auto db = cobble::structured::SingleDb::Open(config);
  VerifyBatchAndMultiGet(db);
  VerifyScan(db);
  const auto snapshot = db.TakeSnapshot();
  auto empty_bounds_plan =
      cobble::structured::ScanPlan::FromGlobalSnapshot(snapshot);
  empty_bounds_plan.WithStart(cobble::BytesView{}).WithEnd(cobble::BytesView{});
  const auto empty_bound_splits = empty_bounds_plan.Splits();
  COBBLE_CHECK(!empty_bound_splits.empty());
  COBBLE_CHECK(empty_bound_splits.front().start_inclusive.has_value());
  COBBLE_CHECK(empty_bound_splits.front().start_inclusive->empty());
  COBBLE_CHECK(empty_bound_splits.front().end_exclusive.has_value());
  COBBLE_CHECK(empty_bound_splits.front().end_exclusive->empty());
  const auto empty_bound_round_trip = cobble::structured::ScanSplit::FromJson(
      empty_bound_splits.front().ToJson());
  COBBLE_CHECK(empty_bound_round_trip.start_inclusive.has_value());
  COBBLE_CHECK(empty_bound_round_trip.start_inclusive->empty());
  COBBLE_CHECK(empty_bound_round_trip.end_exclusive.has_value());
  COBBLE_CHECK(empty_bound_round_trip.end_exclusive->empty());
  auto plan = cobble::structured::ScanPlan::FromGlobalSnapshot(snapshot);
  plan.WithStart(Bytes("key-")).WithEnd(Bytes("key."));
  auto splits = plan.Splits();
  COBBLE_CHECK(!splits.empty());
  const auto json = splits.front().ToJson();
  auto split = cobble::structured::ScanSplit::FromJson(json);
  auto malformed = json;
  const auto marker = malformed.find("\"start_bucket\":null");
  COBBLE_CHECK(marker != std::string::npos);
  malformed.replace(marker, std::strlen("\"start_bucket\":null"),
                    "\"start_bucket\":0");
  bool malformed_rejected = false;
  try {
    (void)cobble::structured::ScanSplit::FromJson(malformed);
  } catch (const cobble::Error &error) {
    malformed_rejected = error.code() == cobble::ErrorCode::kInput;
  }
  COBBLE_CHECK(malformed_rejected);
  const std::array<cobble::Byte, 3> binary = {0, 0xff, 1};
  const auto partition = split.SplitAfter(0, binary);
  COBBLE_CHECK(partition.after.start_after_exclusive.has_value());
  auto scanner = split.OpenScannerFile(config_path.string());
  std::size_t scanned = 0;
  for (;;) {
    auto batch = scanner.Next(29);
    scanned += batch.RowCount();
    if (batch.End())
      break;
  }
  COBBLE_CHECK(scanned == 320);

  cobble::structured::ScanOptions unsupported;
  unsupported.SetStopAtBlockBoundary(true);
  bool rejected = false;
  try {
    (void)split.OpenScanner(config, unsupported);
  } catch (const cobble::Error &error) {
    rejected = error.code() == cobble::ErrorCode::kInput;
  }
  COBBLE_CHECK(rejected);
  db.Close();
}

} // namespace

int main() {
  try {
    cobble_test::TempDirectory root("cobble-cpp-structured-batch-scan");
    VerifySharded(root.path() / "sharded");
    VerifySingleAndPlan(root.path() / "single");
    std::cout << "structured C++ batch/scan capability test passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
