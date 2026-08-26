#include "test_support.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
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
  json += R"(],"num_columns":1,"total_buckets":)" +
          std::to_string(buckets) +
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
    rejected = error.code() == cobble::ErrorCode::kInput ||
               error.code() == cobble::ErrorCode::kConfiguration;
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
  COBBLE_CHECK(String(old.column(0)) == "old-value");
  COBBLE_CHECK(String(old.column(1)) == "default-column");

  db.Put(0, Bytes("merge"), 0, Bytes("base"));
  db.Merge(0, Bytes("merge"), 0, Bytes("-merged"));
  COBBLE_CHECK(String(db.Get(0, Bytes("merge")).column(0)) ==
               "base-merged");
  db.Put(1, Bytes("delete"), 0, Bytes("value"));
  db.Delete(1, Bytes("delete"), 0);
  COBBLE_CHECK(!db.Get(1, Bytes("delete")).found());

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
  COBBLE_CHECK(rows.row_count() == keys.size());
  COBBLE_CHECK(String(rows.column(0, 0)) == "old-value");
  COBBLE_CHECK(String(rows.column(1, 0)) == "two");
  COBBLE_CHECK(String(rows.column(2, 0)) == "four");
  COBBLE_CHECK(String(rows.column(3, 0)) == "two");
  COBBLE_CHECK(!rows.found(4));

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
  COBBLE_CHECK(batch_rows.row_count() == 3);
  COBBLE_CHECK(String(batch_rows.key(0)) == "scan-a");
  COBBLE_CHECK(String(batch_rows.key(2)) == "scan-c");

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
  COBBLE_CHECK(std::any_of(metrics.begin(), metrics.end(),
                           [](const cobble::MetricSample& sample) {
                             return std::any_of(
                                 sample.labels.begin(), sample.labels.end(),
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
    const auto pending_id = pending.id();
    const bool cancelled = db.CancelSnapshot(pending_id);
    bool wait_cancelled = false;
    try {
      const auto completed = pending.Wait();
      COBBLE_CHECK(!cancelled);
      COBBLE_CHECK(completed.snapshot_id == pending_id);
    } catch (const cobble::Error& error) {
      wait_cancelled = error.code() == cobble::ErrorCode::kCancelled;
    }
    COBBLE_CHECK(wait_cancelled == cancelled);

    recovery = db.TakeSnapshot();
    COBBLE_CHECK(db.GetShardSnapshot(recovery.snapshot_id).manifest_path ==
                 recovery.manifest_path);
    COBBLE_CHECK(db.RetainSnapshot(recovery.snapshot_id));
    db.Put(5, Bytes("wal-tail"), 0, Bytes("after-recovery-snapshot"));
    db.Close();
  }

  {
    auto latest = cobble::Db::Resume(config, db_id);
    COBBLE_CHECK(latest.Get(5, Bytes("wal-tail")).found());
    latest.Close();
  }

  {
    auto exact =
        cobble::Db::ResumeFromSnapshot(config, recovery.snapshot_id, db_id);
    COBBLE_CHECK(!exact.Get(5, Bytes("wal-tail")).found());

    {
      auto cursor = exact.Scan(0, std::nullopt, std::nullopt);
      bool rejected = false;
      try {
        exact.SwitchToSnapshot(recovery.snapshot_id);
      } catch (const cobble::Error& error) {
        rejected = error.code() == cobble::ErrorCode::kInvalidState;
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
        rejected = error.code() == cobble::ErrorCode::kInvalidState;
      }
      COBBLE_CHECK(rejected);
      (void)builder;
    }

    exact.Put(5, Bytes("newer"), 0, Bytes("newer-state"));
    const auto newer = exact.TakeSnapshot();
    COBBLE_CHECK(newer.snapshot_id > recovery.snapshot_id);
    exact.SwitchToSnapshot(recovery.snapshot_id);
    COBBLE_CHECK(!exact.Get(5, Bytes("newer")).found());
    exact.Close();
  }

  {
    auto exact = cobble::Db::ResumeFromSnapshotFile(
        config_path.string(), recovery.snapshot_id, db_id);
    COBBLE_CHECK(!exact.Get(5, Bytes("wal-tail")).found());
    exact.Close();
  }

  {
    auto opened = cobble::Db::OpenFromSnapshot(
        config, recovery.snapshot_id, db_id,
        cobble::RecoveryMode::kSnapshotOnly);
    COBBLE_CHECK(!opened.Get(5, Bytes("wal-tail")).found());
    opened.Close();
  }

  return {db_id, recovery};
}

void VerifyRestoreNew(std::string_view config,
                      const MainSnapshotState& source) {
  {
    auto restored = cobble::Db::RestoreNew(
        config, source.recovery.snapshot_id, source.db_id);
    COBBLE_CHECK(restored.Id() != source.db_id);
    COBBLE_CHECK(restored.Get(4, Bytes("batch-four")).found());
    restored.Close();
  }
  {
    auto restored = cobble::Db::RestoreNewFromManifest(
        config, source.recovery.manifest_path);
    COBBLE_CHECK(restored.Id() != source.db_id);
    COBBLE_CHECK(restored.Get(2, Bytes("batch-two")).found());
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
  (void)target.ExpandBucket(
      source.Id(), source_snapshot.snapshot_id, persistent_ranges,
      cobble::ExpandStorageMode::kReferencePersistent);
  COBBLE_CHECK(String(target.Get(2, Bytes("source-two")).column(0)) == "two");
  target.WaitForExpandAdoption(std::chrono::seconds(1));

  const std::array adopted_ranges = {cobble::BucketRange{4, 5}};
  (void)target.ExpandBucket(source.Id(), source_snapshot.snapshot_id,
                            adopted_ranges,
                            cobble::ExpandStorageMode::kAdoptAsync);
  target.WaitForExpandAdoption(std::chrono::seconds(10));
  source.Close();
  COBBLE_CHECK(String(target.Get(4, Bytes("source-four")).column(0)) ==
               "four");

  const std::array shrink_ranges = {cobble::BucketRange{2, 5}};
  (void)target.ShrinkBucket(shrink_ranges);
  bool removed = false;
  try {
    removed = !target.Get(2, Bytes("source-two")).found();
  } catch (const cobble::Error&) {
    removed = true;
  }
  COBBLE_CHECK(removed);

  target.Put(0, Bytes("post-shrink"), 0, Bytes("kept"));
  const auto post_shrink = target.TakeSnapshot();
  const auto target_id = target.Id();
  target.Close();

  auto resumed = cobble::Db::ResumeFromSnapshot(
      config, post_shrink.snapshot_id, target_id);
  COBBLE_CHECK(resumed.Get(0, Bytes("post-shrink")).found());
  resumed.Close();
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

    cobble_test::TempDirectory rescale_directory(
        "cobble-cpp-sharded-rescale");
    VerifyRescale(rescale_directory.path());

    std::cout << "verified sharded C++ Db capability surface\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "sharded Db capability test failed: " << error.what()
              << '\n';
    return 1;
  }
}
