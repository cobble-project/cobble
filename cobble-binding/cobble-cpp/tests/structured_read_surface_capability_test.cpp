#include "test_support.hpp"

#include <cobble/coordinator.hpp>
#include <cobble/structured.hpp>

#include <array>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;
using cobble_test::String;

std::string Config(const std::filesystem::path &root) {
  return R"({"volumes":[{"base_dir":")" + FileUrl(root) +
         R"(","kinds":["meta","primary_data_priority_high","snapshot"]}],"num_columns":1,"total_buckets":4,"memtable_capacity":"8KB","base_file_size":"16KB","block_cache_size":0,"wal_enabled":false})";
}

void CheckCsrb(const std::vector<cobble::Byte> &buffer,
               const cobble::BufferResult &result, std::size_t count) {
  COBBLE_CHECK(result.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(result.row_count == count);
  COBBLE_CHECK(result.bytes_written == result.bytes_required);
  COBBLE_CHECK(result.bytes_written >= 24);
  COBBLE_CHECK(std::memcmp(buffer.data(), "CSRB", 4) == 0);
}

template <typename Database>
void CheckReads(Database &db) {
  COBBLE_CHECK(db.CurrentSchema().Type("default", 1).kind ==
               cobble::structured::ColumnKind::kList);
  auto row = db.Get(0, Bytes("both"));
  COBBLE_CHECK(row.Found() && row.ColumnCount() == 2);
  COBBLE_CHECK(String(row.Bytes(0)) == "old");
  COBBLE_CHECK(row.ListSize(1) == 2);
  COBBLE_CHECK(String(row.ListElement(1, 1)) == "two");
  COBBLE_CHECK(!db.Get(0, Bytes("absent")).Found());
  COBBLE_CHECK(!db.Get(0, Bytes("null-list")).HasColumn(1));

  cobble::structured::ReadOptions projected;
  const std::array<std::size_t, 1> one = {1};
  projected.SetColumns(one);
  auto list = db.Get(0, Bytes("both"), projected);
  COBBLE_CHECK(list.ColumnCount() == 1 && list.ListSize(0) == 2);
  const std::array keys = {
      cobble::structured::MultiGetKey{0, Bytes("both")},
      cobble::structured::MultiGetKey{2, Bytes("right")},
      cobble::structured::MultiGetKey{0, Bytes("both")},
      cobble::structured::MultiGetKey{1, Bytes("absent")},
  };
  auto rows = db.MultiGet(keys);
  COBBLE_CHECK(rows.RowCount() == 4);
  COBBLE_CHECK(rows.Found(0) && rows.Found(1) && rows.Found(2));
  COBBLE_CHECK(!rows.Found(3));
  COBBLE_CHECK(String(rows.Bytes(1, 0)) == "old-right");
  COBBLE_CHECK(db.MultiGet(keys, projected).ListSize(0, 0) == 2);

  std::array<cobble::Byte, 1> tiny = {0xa5};
  auto needed = db.GetInto(0, Bytes("both"), tiny);
  COBBLE_CHECK(needed.status == cobble::BufferStatus::kBufferTooSmall);
  COBBLE_CHECK(tiny[0] == 0xa5);
  std::vector<cobble::Byte> output(needed.bytes_required);
  CheckCsrb(output, db.GetInto(0, Bytes("both"), output), 1);
  needed = db.MultiGetInto(keys, tiny);
  COBBLE_CHECK(needed.status == cobble::BufferStatus::kBufferTooSmall);
  COBBLE_CHECK(tiny[0] == 0xa5);
  output.resize(needed.bytes_required);
  CheckCsrb(output, db.MultiGetInto(keys, output), keys.size());
}

}  // namespace

int main() {
  try {
    cobble_test::TempDirectory directory("cobble-cpp-structured-read-surface");
    const auto config = Config(directory.path() / "database");
    const auto path = directory.path() / "config.json";
    {
      std::ofstream file(path);
      file << config;
    }
    const std::array left_range = {cobble::BucketRange{0, 1}};
    const std::array right_range = {cobble::BucketRange{2, 3}};
    auto left = cobble::structured::Db::Open(config, left_range);
    auto right = cobble::structured::Db::Open(config, right_range);
    for (auto *db : {&left, &right}) {
      auto schema = db->UpdateSchema();
      schema.AddListColumn(std::nullopt, 1, {});
      (void)schema.Commit();
    }
    left.PutBytes(0, Bytes("both"), 0, Bytes("old"));
    const std::array<cobble::BytesView, 2> elements = {Bytes("one"),
                                                       Bytes("two")};
    left.PutList(0, Bytes("both"), 1, elements);
    left.PutBytes(0, Bytes("null-list"), 0, Bytes("only-bytes"));
    right.PutBytes(2, Bytes("right"), 0, Bytes("old-right"));
    const auto left_first = left.TakeSnapshot();
    const auto right_first = right.TakeSnapshot();
    auto coordinator = cobble::DbCoordinator::Open(config);
    const std::array first_shards = {left_first, right_first};
    const auto first =
        coordinator.MaterializeGlobalSnapshot(4, 100, first_shards);
    COBBLE_CHECK(coordinator.RetainSnapshot(first.id));

    const auto db_root = directory.path() / "database";
    const auto global_path =
        db_root / "snapshot" / ("SNAPSHOT-" + std::to_string(first.id));
    const auto data_dir = db_root / left.Id() / "data";
    COBBLE_CHECK(std::filesystem::is_directory(data_dir));
    cobble::GlobalSnapshot loaded;
    {
      cobble_test::ScopedRename unavailable(data_dir,
                                            data_dir.string() + ".hidden");
      const auto loaded_shard = cobble::LoadShardSnapshotMetadata(
          config, left.Id(), left_first.manifest_path);
      COBBLE_CHECK(loaded_shard.has_schema_metadata);
      COBBLE_CHECK(loaded_shard.schema_id == left_first.schema_id);
      COBBLE_CHECK(cobble::ShardSnapshot::FromJson(loaded_shard.ToJson())
                       .schema_column_families.size() ==
                   loaded_shard.schema_column_families.size());
      loaded = cobble::LoadGlobalSnapshotMetadataFile(path.string(),
                                                      FileUrl(global_path));
      COBBLE_CHECK(loaded.id == first.id);
      COBBLE_CHECK(!loaded.shards.front().has_schema_metadata);
      loaded = cobble::GlobalSnapshot::FromJson(loaded.ToJson());
      unavailable.Restore();
    }

    const std::array<cobble::Byte, 2> binary_start = {0x00, 0xff};
    const std::array<cobble::Byte, 2> binary_end = {0xff, 0x00};
    auto scan_plan = cobble::structured::ScanPlan::FromGlobalSnapshot(loaded);
    scan_plan.WithStart(binary_start).WithEnd(binary_end);
    const auto transferred_splits =
        cobble::structured::ScanPlan::FromJson(scan_plan.ToJson()).Splits();
    COBBLE_CHECK(transferred_splits.size() == 2);
    COBBLE_CHECK(
        transferred_splits[0].start_inclusive ==
        std::vector<cobble::Byte>(binary_start.begin(), binary_start.end()));
    COBBLE_CHECK(
        transferred_splits[0].end_exclusive ==
        std::vector<cobble::Byte>(binary_end.begin(), binary_end.end()));
    COBBLE_CHECK(transferred_splits[0]
                     .OpenScannerFile(path.string())
                     .Next(10)
                     .RowCount() == 2);

    auto fixed = cobble::structured::ReadOnlyDb::OpenFile(
        path.string(), left_first.snapshot_id, left.Id());
    COBBLE_CHECK(fixed.Id() == left.Id());
    COBBLE_CHECK(fixed.Get(0, Bytes("both")).ListSize(1) == 2);
    std::array<cobble::Byte, 1> tiny{};
    auto needed = fixed.GetInto(0, Bytes("both"), tiny);
    COBBLE_CHECK(needed.status == cobble::BufferStatus::kBufferTooSmall);
    std::vector<cobble::Byte> output(needed.bytes_required);
    CheckCsrb(output, fixed.GetInto(0, Bytes("both"), output), 1);
    const std::array fixed_keys = {
        cobble::structured::MultiGetKey{0, Bytes("both")},
        cobble::structured::MultiGetKey{0, Bytes("absent")}};
    COBBLE_CHECK(fixed.MultiGet(fixed_keys).RowCount() == 2);
    needed = fixed.MultiGetInto(fixed_keys, tiny);
    output.resize(needed.bytes_required);
    CheckCsrb(output, fixed.MultiGetInto(fixed_keys, output), 2);
    auto fixed_cursor = [&] {
      auto temporary = cobble::structured::ReadOnlyDb::Open(
          config, left_first.snapshot_id, left.Id());
      return temporary.Scan(0, Bytes("a"), Bytes("z"));
    }();
    COBBLE_CHECK(fixed_cursor.Next(10).RowCount() == 2);

    auto pinned = cobble::structured::Reader::OpenFile(path.string(), first.id);
    auto current = cobble::structured::Reader::OpenCurrent(config);
    auto wrong_size_config = config;
    const auto bucket_count = wrong_size_config.find("\"total_buckets\":4");
    COBBLE_CHECK(bucket_count != std::string::npos);
    wrong_size_config.replace(bucket_count, sizeof("\"total_buckets\":4") - 1,
                              "\"total_buckets\":1");
    cobble_test::ScopedRename unavailable(global_path,
                                          global_path.string() + ".hidden");
    auto from_object =
        cobble::structured::Reader::Open(wrong_size_config, loaded);
    auto from_object_file =
        cobble::structured::Reader::OpenFile(path.string(), loaded);
    COBBLE_CHECK(from_object.ConfiguredSnapshotId() == first.id);
    COBBLE_CHECK(String(from_object.Get(2, Bytes("right")).Bytes(0)) ==
                 "old-right");
    COBBLE_CHECK(String(from_object_file.Get(0, Bytes("both")).Bytes(0)) ==
                 "old");
    unavailable.Restore();
    COBBLE_CHECK(pinned.Mode() == cobble::structured::ReaderMode::kSnapshot);
    COBBLE_CHECK(pinned.ConfiguredSnapshotId() == first.id);
    COBBLE_CHECK(current.Mode() == cobble::structured::ReaderMode::kCurrent);
    COBBLE_CHECK(!current.ConfiguredSnapshotId());
    COBBLE_CHECK(current.CurrentGlobalSnapshot().id == first.id);
    COBBLE_CHECK(!current.ListGlobalSnapshots().empty());
    CheckReads(current);
    auto cursor = [&] {
      auto temporary = cobble::structured::Reader::Open(config, first.id);
      return temporary.Scan(0, Bytes("a"), Bytes("z"));
    }();
    std::array<cobble::Byte, 1> scan_tiny{};
    needed = cursor.NextBatchInto(10, scan_tiny);
    COBBLE_CHECK(needed.status == cobble::BufferStatus::kBufferTooSmall);
    output.resize(needed.bytes_required);
    CheckCsrb(output, cursor.NextBatchInto(10, output), 2);

    cobble::structured::ScanOptions scan_projected;
    const std::array<std::size_t, 1> bytes_column = {0};
    scan_projected.SetColumns(bytes_column);
    auto retained_scan =
        current.Scan(0, Bytes("a"), Bytes("z"), scan_projected);

    left.PutBytes(0, Bytes("both"), 0, Bytes("new"));
    right.PutBytes(2, Bytes("right"), 0, Bytes("new-right"));
    auto schema = left.UpdateSchema();
    schema.AddListColumn(std::nullopt, 2, {});
    (void)schema.Commit();
    auto right_schema = right.UpdateSchema();
    right_schema.AddListColumn(std::nullopt, 2, {});
    (void)right_schema.Commit();
    const std::array second_shards = {left.TakeSnapshot(),
                                      right.TakeSnapshot()};
    const auto second =
        coordinator.MaterializeGlobalSnapshot(4, 101, second_shards);
    bool rejected = false;
    try {
      pinned.Refresh();
    } catch (const cobble::Error &error) {
      rejected = error.Code() == cobble::ErrorCode::kInvalidState;
    }
    COBBLE_CHECK(rejected);
    COBBLE_CHECK(String(pinned.Get(0, Bytes("both")).Bytes(0)) == "old");
    COBBLE_CHECK(String(from_object.Get(0, Bytes("both")).Bytes(0)) == "old");
    COBBLE_CHECK(from_object.Get(0, Bytes("both")).ColumnCount() == 2);
    current.Refresh();
    COBBLE_CHECK(current.CurrentGlobalSnapshot().id == second.id);
    COBBLE_CHECK(current.CurrentSchema().Type("default", 2).kind ==
                 cobble::structured::ColumnKind::kList);
    auto old_batch = retained_scan.Next(10);
    COBBLE_CHECK(old_batch.RowCount() == 2);
    bool saw_old = false;
    for (std::size_t index = 0; index < old_batch.RowCount(); ++index) {
      COBBLE_CHECK(old_batch.ColumnCount(index) == 1);
      if (String(old_batch.Key(index)) == "both") {
        COBBLE_CHECK(String(old_batch.Bytes(index, 0)) == "old");
        saw_old = true;
      }
    }
    COBBLE_CHECK(saw_old);
    auto refreshed = current.Get(0, Bytes("both"));
    COBBLE_CHECK(refreshed.ColumnCount() == 3);
    COBBLE_CHECK(!refreshed.HasColumn(2));
    COBBLE_CHECK(String(refreshed.Bytes(0)) == "new");
    const std::array new_keys = {
        cobble::structured::MultiGetKey{0, Bytes("both")},
        cobble::structured::MultiGetKey{2, Bytes("right")}};
    auto refreshed_rows = current.MultiGet(new_keys);
    COBBLE_CHECK(refreshed_rows.ColumnCount(0) == 3);
    COBBLE_CHECK(refreshed_rows.ColumnCount(1) == 3);
    COBBLE_CHECK(String(current.Get(2, Bytes("right")).Bytes(0)) ==
                 "new-right");
    COBBLE_CHECK(String(fixed.Get(0, Bytes("both")).Bytes(0)) == "old");
    left.Close();
    right.Close();
    std::cout << "structured Reader and ReadOnlyDb capability test passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
