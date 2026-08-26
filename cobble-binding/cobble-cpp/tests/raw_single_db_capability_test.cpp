#include "test_support.hpp"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;
using cobble_test::String;

std::string Config(const std::filesystem::path& root, std::size_t columns = 1) {
  const auto url = FileUrl(root);
  return R"({"volumes":[{"base_dir":")" + url +
         R"(","kinds":["meta","primary_data_priority_high","snapshot"]},{"base_dir":")" +
         url + R"(","kinds":["wal"]}],"num_columns":)" +
         std::to_string(columns) +
         R"(,"total_buckets":4,"memtable_capacity":"8KB","base_file_size":"16KB","block_cache_size":0,"ttl_enabled":true,"time_provider":"manual","wal_enabled":true,"wal_flush_interval_ms":5})";
}

const cobble::ColumnFamily& DefaultFamily(const cobble::Schema& schema) {
  const auto family = std::find_if(
      schema.column_families.begin(), schema.column_families.end(),
      [](const cobble::ColumnFamily& value) { return value.name == "default"; });
  COBBLE_CHECK(family != schema.column_families.end());
  return *family;
}

cobble::OwnedMultiGetResult FetchMixedKeys(const cobble::SingleDb& db) {
  // These strings and descriptors die on return. Only the Rust-owned result
  // payload must remain reachable through the returned RAII object.
  const std::string duplicate = "duplicate";
  const std::string cross_bucket = "cross-bucket";
  const std::string empty;
  const std::string missing = "missing";
  const std::vector<cobble::MultiGetKey> keys = {
      {0, Bytes(duplicate)},   {0, Bytes(duplicate)},
      {0, Bytes(empty)},       {0, Bytes(cross_bucket)},
      {1, Bytes(cross_bucket)}, {3, Bytes(missing)},
  };
  return db.MultiGet(keys);
}

void VerifyDetachedSchemaBuilder(const std::filesystem::path& root) {
  auto owner =
      std::make_unique<cobble::SingleDb>(cobble::SingleDb::Open(Config(root)));
  auto builder =
      std::make_unique<cobble::SchemaBuilder>(owner->UpdateSchema());

  // The builder retains the native database owner. Destroying the public
  // database first must neither deadlock nor invalidate the builder.
  owner.reset();
  builder->AddColumn(1, std::nullopt, Bytes("detached-default"));
  const auto schema = builder->Commit();
  builder.reset();
  COBBLE_CHECK(DefaultFamily(schema).column_count == 2);
}

void VerifySchemaEvolution(cobble::SingleDb& db) {
  const auto initial = db.CurrentSchema();
  const auto& initial_default = DefaultFamily(initial);
  COBBLE_CHECK(initial_default.id == 0);
  COBBLE_CHECK(initial_default.column_count == 1);
  COBBLE_CHECK(initial_default.merge_operators.size() == 1);
  COBBLE_CHECK(!initial_default.merge_operators[0].id.empty());
  COBBLE_CHECK(!initial_default.merge_operators[0].metadata_json.has_value());

  db.Put(0, Bytes("before-schema"), 0, Bytes("old-value"));
  const auto before_schema = db.TakeSnapshot();
  COBBLE_CHECK(before_schema.shards.size() == 1);

  auto builder = db.UpdateSchema();
  auto default_operator = initial_default.merge_operators[0];
  default_operator.metadata_json = R"({"binding":"cpp"})";
  builder.SetColumnOperator(std::nullopt, 0, default_operator);
  builder.AddColumn(1, std::nullopt, Bytes("new-default"));
  builder.SetFamilyTtl(std::nullopt, false);
  const auto evolved = builder.Commit();
  const auto& evolved_default = DefaultFamily(evolved);
  COBBLE_CHECK(evolved.version > initial.version);
  COBBLE_CHECK(evolved_default.column_count == 2);
  COBBLE_CHECK(evolved_default.merge_operators.size() == 2);
  COBBLE_CHECK(!evolved_default.value_has_ttl);

  bool consumed = false;
  try {
    (void)builder.Commit();
  } catch (const cobble::Error& error) {
    consumed = error.code() == cobble::ErrorCode::kInvalidState;
  }
  COBBLE_CHECK(consumed);

  const auto row = db.Get(0, Bytes("before-schema"));
  COBBLE_CHECK(row.found());
  COBBLE_CHECK(row.column_count() == 2);
  COBBLE_CHECK(String(row.column(0)) == "old-value");
  COBBLE_CHECK(String(row.column(1)) == "new-default");

  db.Put(0, Bytes("merge-after-schema"), 0, Bytes("base"));
  db.Merge(0, Bytes("merge-after-schema"), 0, Bytes("-merged"));
  COBBLE_CHECK(String(db.Get(0, Bytes("merge-after-schema")).column(0)) ==
               "base-merged");
}

void VerifyMultiGet(cobble::SingleDb& db) {
  db.Put(0, Bytes("duplicate"), 0, Bytes("duplicate-value"));
  db.Put(0, Bytes(""), 0, Bytes("empty-key-value"));
  db.Put(0, Bytes("cross-bucket"), 0, Bytes("bucket-zero"));
  db.Put(1, Bytes("cross-bucket"), 0, Bytes("bucket-one"));

  auto result = FetchMixedKeys(db);
  COBBLE_CHECK(result.row_count() == 6);
  COBBLE_CHECK(result.found(0));
  COBBLE_CHECK(result.found(1));
  COBBLE_CHECK(String(result.column(0, 0)) == "duplicate-value");
  COBBLE_CHECK(String(result.column(1, 0)) == "duplicate-value");
  COBBLE_CHECK(String(result.column(2, 0)) == "empty-key-value");
  COBBLE_CHECK(String(result.column(3, 0)) == "bucket-zero");
  COBBLE_CHECK(String(result.column(4, 0)) == "bucket-one");
  COBBLE_CHECK(!result.found(5));
  COBBLE_CHECK(result.column_count(5) == 0);
  COBBLE_CHECK(!result.has_column(5, 0));

  // A view obtained after all caller key buffers have been destroyed still
  // points into the result's Rust-owned Bytes allocation.
  const auto retained_view = result.column(4, 0);
  COBBLE_CHECK(String(retained_view) == "bucket-one");

  const std::vector<cobble::MultiGetKey> no_keys;
  COBBLE_CHECK(db.MultiGet(no_keys).row_count() == 0);
}

cobble::GlobalSnapshot VerifyTypedSnapshots(cobble::SingleDb& db) {
  db.Put(2, Bytes("async"), 0, Bytes("snapshot"));
  auto pending = db.StartSnapshot();
  const auto pending_id = pending.id();
  const auto async_snapshot = pending.Wait();
  COBBLE_CHECK(async_snapshot.id == pending_id);
  COBBLE_CHECK(async_snapshot.total_buckets == 4);
  COBBLE_CHECK(async_snapshot.shards.size() == 1);
  COBBLE_CHECK(async_snapshot.shards[0].ranges.size() == 1);
  COBBLE_CHECK(async_snapshot.shards[0].ranges[0].start_inclusive == 0);
  COBBLE_CHECK(async_snapshot.shards[0].ranges[0].end_inclusive == 3);

  bool second_wait_rejected = false;
  try {
    (void)pending.Wait();
  } catch (const cobble::Error& error) {
    second_wait_rejected = error.code() == cobble::ErrorCode::kInvalidState;
  }
  COBBLE_CHECK(second_wait_rejected);

  db.Put(2, Bytes("sync"), 0, Bytes("snapshot"));
  const auto sync_snapshot = db.TakeSnapshot();
  COBBLE_CHECK(sync_snapshot.id > async_snapshot.id);
  COBBLE_CHECK(db.GetSnapshot(sync_snapshot.id).id == sync_snapshot.id);
  const auto snapshots = db.ListGlobalSnapshots();
  COBBLE_CHECK(std::any_of(snapshots.begin(), snapshots.end(),
                           [&](const cobble::GlobalSnapshot& snapshot) {
                             return snapshot.id == sync_snapshot.id;
                           }));
  COBBLE_CHECK(db.RetainSnapshot(sync_snapshot.id));
  COBBLE_CHECK(db.ExpireSnapshot(async_snapshot.id));
  return sync_snapshot;
}

void VerifyLifecycleAndMetrics(cobble::SingleDb& db) {
  db.SetTime(2'000);
  COBBLE_CHECK(db.NowSeconds() == 2'000);
  db.SwitchMemtableType(cobble::MemtableType::kSkiplist, true);
  db.SwitchMemtableType(cobble::MemtableType::kAdaptive, false);
  COBBLE_CHECK(db.LoadReadonlyFilesToPrimary() == 0);

  const auto metrics = db.Metrics();
  COBBLE_CHECK(!metrics.empty());
  bool saw_database_label = false;
  bool saw_typed_value = false;
  for (const auto& sample : metrics) {
    saw_database_label =
        saw_database_label ||
        std::any_of(sample.labels.begin(), sample.labels.end(),
                    [](const cobble::MetricLabel& label) {
                      return label.key == "db_id" && !label.value.empty();
                    });
    saw_typed_value = saw_typed_value ||
                      std::holds_alternative<cobble::CounterValue>(sample.value) ||
                      std::holds_alternative<cobble::GaugeValue>(sample.value) ||
                      std::holds_alternative<cobble::HistogramValue>(sample.value);
  }
  COBBLE_CHECK(saw_database_label);
  COBBLE_CHECK(saw_typed_value);
}

}  // namespace

int main() {
  try {
    cobble_test::TempDirectory detached("cobble-cpp-detached-schema");
    VerifyDetachedSchemaBuilder(detached.path() / "database");

    cobble_test::TempDirectory directory("cobble-cpp-raw-capability");
    const auto config = Config(directory.path() / "database");
    cobble::SnapshotId recovery_snapshot = 0;
    {
      cobble::SingleDb db = cobble::SingleDb::Open(config);
      VerifySchemaEvolution(db);
      VerifyMultiGet(db);
      const auto snapshot = VerifyTypedSnapshots(db);
      recovery_snapshot = snapshot.id;
      VerifyLifecycleAndMetrics(db);
      db.Put(3, Bytes("wal-tail"), 0, Bytes("after-snapshot"));
      db.Close();
    }

    {
      auto latest = cobble::SingleDb::Resume(
          config, recovery_snapshot, cobble::RecoveryMode::kLatestWithWal);
      COBBLE_CHECK(latest.Get(3, Bytes("wal-tail")).found());
      latest.Close();
    }
    {
      auto exact = cobble::SingleDb::Resume(
          config, recovery_snapshot, cobble::RecoveryMode::kSnapshotOnly);
      COBBLE_CHECK(!exact.Get(3, Bytes("wal-tail")).found());
      COBBLE_CHECK(exact.Get(2, Bytes("sync")).found());
      exact.Close();
    }

    std::cout << "verified raw C++ SingleDb capability surface\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "raw SingleDb capability test failed: " << error.what()
              << '\n';
    return 1;
  }
}
