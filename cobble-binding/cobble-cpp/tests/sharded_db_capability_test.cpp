#include "test_support.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;
using cobble_test::String;

std::string Config(const std::filesystem::path& root, std::uint32_t buckets,
                   bool wal_enabled) {
  const auto url = FileUrl(root);
  std::string json =
      R"({"volumes":[{"base_dir":")" + url +
      R"(","kinds":["meta","primary_data_priority_high","snapshot"]})";
  if (wal_enabled) {
    json += R"(,{"base_dir":")" + url + R"(","kinds":["wal"]})";
  }
  json +=
      R"(],"num_columns":1,"total_buckets":)" + std::to_string(buckets) +
      R"(,"memtable_capacity":"8KB","base_file_size":"16KB","block_cache_size":0,"snapshot_retention":20,"ttl_enabled":true,"time_provider":"manual","wal_enabled":)" +
      std::string(wal_enabled ? "true" : "false") +
      R"(,"wal_flush_interval_ms":5})";
  return json;
}

void WriteFile(const std::filesystem::path& path, std::string_view contents) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  COBBLE_CHECK(output.is_open());
  output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  output.close();
  COBBLE_CHECK(output.good());
}

cobble::ShardSnapshot WaitForSnapshot(const cobble::Db& db,
                                      cobble::SnapshotId snapshot) {
  for (std::size_t attempt = 0; attempt < 3'000; ++attempt) {
    try {
      return db.GetShardSnapshot(snapshot);
    } catch (const cobble::Error&) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  throw std::runtime_error("shard snapshot did not materialize");
}

void ExpectInputError(const auto& action) {
  bool rejected = false;
  try {
    action();
  } catch (const cobble::Error& error) {
    rejected = error.Code() == cobble::ErrorCode::kInput ||
               error.Code() == cobble::ErrorCode::kConfiguration;
  }
  COBBLE_CHECK(rejected);
}

void VerifyRangeValidation(std::string_view config) {
  const std::vector<cobble::BucketRange> empty;
  ExpectInputError([&] { (void)cobble::Db::Open(config, empty); });
  const std::array reversed = {cobble::BucketRange{3, 2}};
  ExpectInputError([&] { (void)cobble::Db::Open(config, reversed); });
  const std::array outside = {cobble::BucketRange{0, 6}};
  ExpectInputError([&] { (void)cobble::Db::Open(config, outside); });
}

void VerifyCrudAndSchema(cobble::Db& db) {
  db.Put(0, Bytes("old-row"), 0, Bytes("old-value"));
  const auto before_schema = db.TakeSnapshot();
  COBBLE_CHECK(before_schema.ranges.size() == 1);
  COBBLE_CHECK(before_schema.ranges[0].start_inclusive == 0);
  COBBLE_CHECK(before_schema.ranges[0].end_inclusive == 5);

  auto builder = db.UpdateSchema();
  const auto schema = db.CurrentSchema();
  COBBLE_CHECK(schema.column_families.size() == 1);
  auto merge_operator = schema.column_families[0].merge_operators[0];
  merge_operator.metadata_json = R"({"binding":"cpp-sharded"})";
  builder.SetColumnOperator(std::nullopt, 0, merge_operator);
  builder.AddColumn(1, std::nullopt, Bytes("default-column"));
  const auto evolved = builder.Commit();
  COBBLE_CHECK(evolved.column_families[0].column_count == 2);
  const auto old = db.Get(0, Bytes("old-row"));
  COBBLE_CHECK(String(old.Column(0)) == "old-value");
  COBBLE_CHECK(String(old.Column(1)) == "default-column");

  db.Put(0, Bytes("merge"), 0, Bytes("base"));
  db.Merge(0, Bytes("merge"), 0, Bytes("-merged"));
  COBBLE_CHECK(String(db.Get(0, Bytes("merge")).Column(0)) == "base-merged");
  db.Put(1, Bytes("delete"), 0, Bytes("value"));
  db.Delete(1, Bytes("delete"), 0);
  COBBLE_CHECK(!db.Get(1, Bytes("delete")).Found());

  cobble::WriteBatch batch;
  batch.Put(2, Bytes("batch-two"), 0, Bytes("two"));
  batch.Put(4, Bytes("batch-four"), 0, Bytes("four"));
  db.Write(std::move(batch));

  const std::array keys = {
      cobble::MultiGetKey{0, Bytes("old-row")},
      cobble::MultiGetKey{2, Bytes("batch-two")},
      cobble::MultiGetKey{4, Bytes("batch-four")},
      cobble::MultiGetKey{2, Bytes("batch-two")},
      cobble::MultiGetKey{5, Bytes("missing")},
  };
  const auto rows = db.MultiGet(keys);
  COBBLE_CHECK(rows.RowCount() == keys.size());
  COBBLE_CHECK(String(rows.Column(0, 0)) == "old-value");
  COBBLE_CHECK(String(rows.Column(1, 0)) == "two");
  COBBLE_CHECK(String(rows.Column(2, 0)) == "four");
  COBBLE_CHECK(String(rows.Column(3, 0)) == "two");
  COBBLE_CHECK(!rows.Found(4));

  cobble::ReadOptions one_column;
  one_column.columns = {0};
  std::array<std::uint8_t, 2> small{};
  const auto needed =
      db.GetColumnInto(4, Bytes("batch-four"), small, one_column);
  COBBLE_CHECK(needed.status == cobble::BufferStatus::kBufferTooSmall);
  std::vector<std::uint8_t> output(needed.bytes_required);
  const auto copied =
      db.GetColumnInto(4, Bytes("batch-four"), output, one_column);
  COBBLE_CHECK(copied.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(String(output) == "four");

  for (std::string_view key : {"scan-a", "scan-b", "scan-c"}) {
    db.Put(3, Bytes(key), 0, Bytes(key));
  }
  auto scan = db.Scan(3, Bytes("scan-"), Bytes("scan."));
  const auto batch_rows = scan.Next(10);
  COBBLE_CHECK(batch_rows.RowCount() == 3);
  COBBLE_CHECK(String(batch_rows.Key(0)) == "scan-a");
  COBBLE_CHECK(String(batch_rows.Key(2)) == "scan-c");

  auto encoded_scan = db.Scan(3, Bytes("scan-"), Bytes("scan."));
  std::array<std::uint8_t, 1> tiny{};
  const auto pending = encoded_scan.NextBatchInto(10, tiny);
  COBBLE_CHECK(pending.status == cobble::BufferStatus::kBufferTooSmall);
  COBBLE_CHECK(pending.bytes_required > tiny.size());
  std::vector<std::uint8_t> encoded(pending.bytes_required);
  const auto encoded_result = encoded_scan.NextBatchInto(10, encoded);
  COBBLE_CHECK(encoded_result.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(encoded_result.bytes_written == encoded.size());
  COBBLE_CHECK(encoded_result.row_count == 3);

  db.SetTime(4'000);
  COBBLE_CHECK(db.NowSeconds() == 4'000);
  db.SwitchMemtableType(cobble::MemtableType::kSkiplist, true);
  COBBLE_CHECK(db.LoadReadonlyFilesToPrimary() == 0);
  const auto metrics = db.Metrics();
  COBBLE_CHECK(std::any_of(
      metrics.begin(), metrics.end(), [](const cobble::MetricSample& sample) {
        return std::any_of(sample.labels.begin(), sample.labels.end(),
                           [](const cobble::MetricLabel& label) {
                             return label.key == "db_id" &&
                                    !label.value.empty();
                           });
      }));
}

struct MainSnapshotState {
  std::string db_id;
  cobble::ShardSnapshot recovery;
};

MainSnapshotState VerifySnapshotsAndRecovery(
    std::string_view config, const std::filesystem::path& config_path) {
  std::string db_id;
  cobble::ShardSnapshot recovery;
  {
    auto db = cobble::Db::Open(config);
    db_id = db.Id();
    COBBLE_CHECK(!db_id.empty());
    VerifyCrudAndSchema(db);

    const auto convenience_id = db.Snapshot();
    const auto convenience = WaitForSnapshot(db, convenience_id);
    COBBLE_CHECK(convenience.snapshot_id == convenience_id);
    COBBLE_CHECK(db.ExpireSnapshot(convenience_id));

    db.Put(5, Bytes("cancel"), 0, Bytes("candidate"));
    auto pending = db.StartSnapshot();
    const auto pending_id = pending.Id();
    const bool cancelled = db.CancelSnapshot(pending_id);
    bool wait_cancelled = false;
    try {
      const auto completed = pending.Wait();
      COBBLE_CHECK(!cancelled);
      COBBLE_CHECK(completed.snapshot_id == pending_id);
    } catch (const cobble::Error& error) {
      wait_cancelled = error.Code() == cobble::ErrorCode::kCancelled;
    }
    COBBLE_CHECK(wait_cancelled == cancelled);

    recovery = db.TakeSnapshot();
    COBBLE_CHECK(recovery.has_schema_metadata);
    COBBLE_CHECK(!recovery.schema_column_families.empty());
    COBBLE_CHECK(recovery.schema_column_families.front().num_columns > 0);
    COBBLE_CHECK(db.GetShardSnapshot(recovery.snapshot_id).manifest_path ==
                 recovery.manifest_path);
    COBBLE_CHECK(db.RetainSnapshot(recovery.snapshot_id));
    db.Put(5, Bytes("wal-tail"), 0, Bytes("after-recovery-snapshot"));
    db.Close();
  }

  {
    auto latest = cobble::Db::Resume(config, db_id);
    COBBLE_CHECK(latest.Get(5, Bytes("wal-tail")).Found());
    latest.Close();
  }

  {
    auto exact =
        cobble::Db::ResumeFromSnapshot(config, recovery.snapshot_id, db_id);
    COBBLE_CHECK(!exact.Get(5, Bytes("wal-tail")).Found());

    {
      auto cursor = exact.Scan(0, std::nullopt, std::nullopt);
      bool rejected = false;
      try {
        exact.SwitchToSnapshot(recovery.snapshot_id);
      } catch (const cobble::Error& error) {
        rejected = error.Code() == cobble::ErrorCode::kInvalidState;
      }
      COBBLE_CHECK(rejected);
      (void)cursor;
    }
    {
      auto builder = exact.UpdateSchema();
      bool rejected = false;
      try {
        exact.SwitchToSnapshot(recovery.snapshot_id);
      } catch (const cobble::Error& error) {
        rejected = error.Code() == cobble::ErrorCode::kInvalidState;
      }
      COBBLE_CHECK(rejected);
      (void)builder;
    }

    exact.Put(5, Bytes("newer"), 0, Bytes("newer-state"));
    const auto newer = exact.TakeSnapshot();
    COBBLE_CHECK(newer.snapshot_id > recovery.snapshot_id);
    exact.SwitchToSnapshot(recovery.snapshot_id);
    COBBLE_CHECK(!exact.Get(5, Bytes("newer")).Found());
    exact.Close();
  }

  {
    auto exact = cobble::Db::ResumeFromSnapshotFile(
        config_path.string(), recovery.snapshot_id, db_id);
    COBBLE_CHECK(!exact.Get(5, Bytes("wal-tail")).Found());
    exact.Close();
  }

  {
    auto opened =
        cobble::Db::OpenFromSnapshot(config, recovery.snapshot_id, db_id,
                                     cobble::RecoveryMode::kSnapshotOnly);
    COBBLE_CHECK(!opened.Get(5, Bytes("wal-tail")).Found());
    opened.Close();
  }

  return {db_id, recovery};
}

void VerifyRestoreNew(std::string_view config,
                      const MainSnapshotState& source) {
  {
    auto restored = cobble::Db::RestoreNew(config, source.recovery.snapshot_id,
                                           source.db_id);
    COBBLE_CHECK(restored.Id() != source.db_id);
    COBBLE_CHECK(restored.Get(4, Bytes("batch-four")).Found());
    restored.Close();
  }
  {
    auto restored = cobble::Db::RestoreNewFromManifest(
        config, source.recovery.manifest_path);
    COBBLE_CHECK(restored.Id() != source.db_id);
    COBBLE_CHECK(restored.Get(2, Bytes("batch-two")).Found());
    restored.Close();
  }
}

void VerifyRescale(const std::filesystem::path& directory) {
  const auto config = Config(directory / "database", 6, false);
  const auto config_path = directory / "rescale-config.json";
  WriteFile(config_path, config);
  const std::array source_ranges = {cobble::BucketRange{4, 5},
                                    cobble::BucketRange{2, 3}};
  const std::array target_ranges = {cobble::BucketRange{0, 1}};

  auto source = cobble::Db::Open(config, source_ranges);
  auto target = cobble::Db::OpenFile(config_path.string(), target_ranges);
  source.Put(2, Bytes("source-two"), 0, Bytes("two"));
  source.Put(4, Bytes("source-four"), 0, Bytes("four"));
  const auto source_snapshot = source.TakeSnapshot();
  COBBLE_CHECK(source.RetainSnapshot(source_snapshot.snapshot_id));

  const std::array persistent_ranges = {cobble::BucketRange{2, 3}};
  (void)target.ExpandBucket(source.Id(), source_snapshot.snapshot_id,
                            persistent_ranges,
                            cobble::ExpandStorageMode::kReferencePersistent);
  COBBLE_CHECK(String(target.Get(2, Bytes("source-two")).Column(0)) == "two");
  target.WaitForExpandAdoption(std::chrono::seconds(1));

  const std::array adopted_ranges = {cobble::BucketRange{4, 5}};
  (void)target.ExpandBucket(source.Id(), source_snapshot.snapshot_id,
                            adopted_ranges,
                            cobble::ExpandStorageMode::kAdoptAsync);
  target.WaitForExpandAdoption(std::chrono::seconds(10));
  source.Close();
  COBBLE_CHECK(String(target.Get(4, Bytes("source-four")).Column(0)) == "four");

  const std::array shrink_ranges = {cobble::BucketRange{2, 5}};
  (void)target.ShrinkBucket(shrink_ranges);
  bool removed = false;
  try {
    removed = !target.Get(2, Bytes("source-two")).Found();
  } catch (const cobble::Error&) {
    removed = true;
  }
  COBBLE_CHECK(removed);

  target.Put(0, Bytes("post-shrink"), 0, Bytes("kept"));
  const auto post_shrink = target.TakeSnapshot();
  const auto target_id = target.Id();
  target.Close();

  auto resumed = cobble::Db::ResumeFromSnapshot(config, post_shrink.snapshot_id,
                                                target_id);
  COBBLE_CHECK(resumed.Get(0, Bytes("post-shrink")).Found());
  resumed.Close();
}

void VerifyExport(const std::filesystem::path& directory,
                  bool capture_current) {
  const auto source = directory / "source";
  const auto destination = directory / "export";
  const auto config = Config(source, 4, false);
  const auto target =
      R"({"base_dir":")" + FileUrl(destination) + R"(","kinds":["snapshot"]})";
  const auto shard_target = R"({"base_dir":")" +
                            FileUrl(destination / "shards") +
                            R"(","kinds":["snapshot"]})";
  const std::array left_range = {cobble::BucketRange{0, 1}};
  const std::array right_range = {cobble::BucketRange{2, 3}};
  auto left = cobble::Db::Open(config, left_range);
  auto right = cobble::Db::Open(config, right_range);
  auto coordinator = cobble::DbCoordinator::Open(config);
  for (cobble::BucketId bucket = 0; bucket < 4; ++bucket) {
    auto& db = bucket < 2 ? left : right;
    for (int index = 0; index < 64; ++index) {
      const auto key = "key-" + std::to_string(index + 100);
      db.Put(bucket, Bytes(key), 0, Bytes(std::string(256, 'a' + bucket)));
    }
  }
  const std::array originals = {left.TakeSnapshot(), right.TakeSnapshot()};
  (void)coordinator.MaterializeGlobalSnapshot(4, 7, originals);
  COBBLE_CHECK(coordinator.RetainSnapshot(7));
  (void)coordinator.MaterializeGlobalSnapshot(4, 8, originals);
  const auto before = coordinator.LoadCurrentGlobalSnapshot()->ToJson();
  ExpectInputError(
      [&] { (void)left.ExportSnapshot(originals[0].snapshot_id, "{"); });
  ExpectInputError([&] { (void)left.SnapshotTo("{"); });
  if (capture_current) {
    for (cobble::BucketId bucket = 0; bucket < 4; ++bucket) {
      (bucket < 2 ? left : right)
          .Put(bucket, Bytes("key-164"), 0,
               Bytes(std::string(256, 'a' + bucket)));
    }
  }
  const auto left_handle =
      capture_current
          ? left.SnapshotTo(shard_target)
          : left.ExportSnapshot(originals[0].snapshot_id, shard_target);
  const auto right_handle =
      capture_current
          ? right.SnapshotTo(shard_target)
          : right.ExportSnapshot(originals[1].snapshot_id, shard_target);
  // Handles must not retain extra DB ownership or prevent Close from draining
  // export I/O.
  left.Close();
  right.Close();
  const std::array exports = {left_handle.Wait(), right_handle.Wait()};
  COBBLE_CHECK(left_handle.Wait().db_id == exports[0].db_id);
  COBBLE_CHECK(right_handle.Wait().db_id == exports[1].db_id);
  COBBLE_CHECK(left_handle.IsFinished() && right_handle.IsFinished());
  COBBLE_CHECK(!left_handle.Cancel() && !right_handle.Cancel());
  const auto global = coordinator.TakeExportedGlobalSnapshot(4, exports);
  COBBLE_CHECK(global.id == 0);
  COBBLE_CHECK(!std::filesystem::exists(destination / "snapshot/SNAPSHOT-0"));
  coordinator.MaterializeExportedGlobalSnapshot(global, target);
  coordinator.MaterializeExportedGlobalSnapshot(global, target);
  auto conflict = global;
  ++conflict.watermark_seconds;
  bool rejected = false;
  try {
    coordinator.MaterializeExportedGlobalSnapshot(conflict, target);
  } catch (const cobble::Error&) {
    rejected = true;
  }
  COBBLE_CHECK(rejected);
  ExpectInputError([&] {
    coordinator.MaterializeExportedGlobalSnapshot(
        global,
        R"({"base_dir":")" + FileUrl(source) + R"(","kinds":["snapshot"]})");
  });
  COBBLE_CHECK(coordinator.LoadCurrentGlobalSnapshot()->ToJson() == before);
  COBBLE_CHECK(coordinator.ListGlobalSnapshots().size() == 2);
  std::filesystem::remove_all(source);
  const auto reader_config = Config(destination, 4, false);
  for (const bool current : {true, false}) {
    auto reader = current ? cobble::Reader::OpenCurrent(reader_config)
                          : cobble::Reader::Open(reader_config, 0);
    for (cobble::BucketId bucket = 0; bucket < 4; ++bucket) {
      const std::string value(256, 'a' + bucket);
      for (int index = 0; index < (capture_current ? 65 : 64); ++index) {
        const auto key = "key-" + std::to_string(index + 100);
        COBBLE_CHECK(String(reader.Get(bucket, Bytes(key)).Column(0)) == value);
      }
      auto scan = reader.Scan(bucket, Bytes("key-"), Bytes("z"));
      std::size_t count = 0;
      while (true) {
        const auto batch = scan.Next(17);
        for (std::size_t index = 0; index < batch.RowCount(); ++index) {
          COBBLE_CHECK(batch.Bucket(index) == bucket);
          COBBLE_CHECK(String(batch.Key(index)) ==
                       "key-" + std::to_string(count++ + 100));
          COBBLE_CHECK(String(batch.Column(index, 0)) == value);
        }
        if (batch.End()) break;
      }
      COBBLE_CHECK(count == (capture_current ? 65 : 64));
    }
  }
}

void VerifySnapshotTo(const std::filesystem::path& directory) {
  const auto source = directory / "source";
  const auto destination = directory / "standalone";
  const auto config =
      R"({"volumes":[{"base_dir":")" + FileUrl(source) +
      R"(","kinds":["meta","primary_data_priority_high","snapshot"]}],"num_columns":1,"total_buckets":4,"wal_enabled":false,"memtable_capacity":"1MB","l0_file_limit":100,"snapshot_on_flush":false,"snapshot_retention":1,"value_separation_threshold":16})";
  const auto target =
      R"({"base_dir":")" + FileUrl(destination) + R"(","kinds":["snapshot"]})";
  const auto manifest = [](const std::filesystem::path& root,
                           const cobble::ShardSnapshot& shard) {
    std::ifstream input(root / shard.db_id / "snapshot" /
                            ("SNAPSHOT-" + std::to_string(shard.snapshot_id)),
                        std::ios::binary);
    COBBLE_CHECK(input.is_open());
    return std::string(std::istreambuf_iterator<char>(input), {});
  };
  const auto base_field = [](cobble::SnapshotId id) {
    return "\"base_snapshot_id\":" + std::to_string(id) + ",";
  };
  auto db = cobble::Db::Open(config);
  const std::string value(128, 'v');
  db.Put(0, Bytes("a"), 0, Bytes(value));
  const auto a = db.TakeSnapshot();
  db.Put(0, Bytes("b"), 0, Bytes(value));
  const auto b = db.TakeSnapshot();
  COBBLE_CHECK(manifest(source, a).find("base_snapshot_id") ==
               std::string::npos);
  COBBLE_CHECK(manifest(source, b).find(base_field(a.snapshot_id)) !=
               std::string::npos);
  db.Put(0, Bytes("c"), 0, Bytes(value));
  const auto handle = db.SnapshotTo(target);
  const auto exported = handle.Wait();
  COBBLE_CHECK(handle.Wait().manifest_path == exported.manifest_path);
  COBBLE_CHECK(handle.IsFinished() && !handle.Cancel());
  COBBLE_CHECK(exported.snapshot_id == b.snapshot_id + 1);
  const auto full = manifest(destination, exported);
  COBBLE_CHECK(full.find("base_snapshot_id") == std::string::npos);
  COBBLE_CHECK(full.find("\"active_memtable_data\":[]") != std::string::npos);
  COBBLE_CHECK(full.find("\"vlog_files\":[]") == std::string::npos);
  COBBLE_CHECK(!std::filesystem::exists(
      source / b.db_id / "snapshot" /
      ("SNAPSHOT-" + std::to_string(exported.snapshot_id))));
  const auto c = db.TakeSnapshot();  // E added only one L0 file since B.
  COBBLE_CHECK(c.snapshot_id == exported.snapshot_id + 1);
  COBBLE_CHECK(manifest(source, c).find(base_field(b.snapshot_id)) !=
               std::string::npos);
  COBBLE_CHECK(!manifest(source, a).empty() && !manifest(source, b).empty());
  db.Put(0, Bytes("d"), 0, Bytes(value));
  db.Close();
  std::filesystem::remove_all(source);
  auto restored = cobble::Db::RestoreNewFromManifest(
      Config(destination, 4, false), exported.manifest_path);
  for (const auto key : {"a", "b", "c"}) {
    COBBLE_CHECK(String(restored.Get(0, Bytes(key)).Column(0)) == value);
  }
  COBBLE_CHECK(!restored.Get(0, Bytes("d")).Found());
  auto scan = restored.Scan(0, std::nullopt, std::nullopt);
  const auto rows = scan.Next(10);
  COBBLE_CHECK(rows.RowCount() == 3 && rows.End());
  for (std::size_t index = 0; index < 3; ++index) {
    COBBLE_CHECK(String(rows.Key(index)) == std::string(1, 'a' + index));
    COBBLE_CHECK(String(rows.Column(index, 0)) == value);
  }
}

}  // namespace

int main() {
  try {
    cobble_test::TempDirectory main_directory("cobble-cpp-sharded");
    const auto config = Config(main_directory.path() / "database", 6, true);
    const auto config_path = main_directory.path() / "config.json";
    WriteFile(config_path, config);
    VerifyRangeValidation(config);
    const auto state = VerifySnapshotsAndRecovery(config, config_path);
    VerifyRestoreNew(config, state);

    cobble_test::TempDirectory rescale_directory("cobble-cpp-sharded-rescale");
    VerifyRescale(rescale_directory.path());
    VerifyExport(main_directory.path() / "export-flow", false);
    VerifyExport(main_directory.path() / "snapshot-to-flow", true);
    VerifySnapshotTo(main_directory.path() / "snapshot-to-chain");

    std::cout << "verified sharded C++ Db capability surface\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "sharded Db capability test failed: " << error.what() << '\n';
    return 1;
  }
}
