#include "test_support.hpp"

#include <cobble/structured.hpp>

#include <array>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;
using cobble_test::String;

std::string Config(const std::filesystem::path &root,
                   std::size_t total_buckets = 4) {
  const auto url = FileUrl(root);
  return R"({"volumes":[{"base_dir":")" + url +
         R"(","kinds":["meta","primary_data_priority_high","snapshot"]},{"base_dir":")" +
         url + R"(","kinds":["wal"]}],"num_columns":1,"total_buckets":)" +
         std::to_string(total_buckets) +
         R"(,"memtable_capacity":"8KB","base_file_size":"16KB","block_cache_size":0,"ttl_enabled":true,"time_provider":"manual","wal_enabled":true,"wal_flush_interval_ms":5})";
}

void VerifyDetachedBuilder(const std::filesystem::path &root) {
  auto owner = std::make_unique<cobble::structured::Db>(
      cobble::structured::Db::Open(Config(root)));
  auto builder = std::make_unique<cobble::structured::SchemaBuilder>(
      owner->UpdateSchema());
  owner.reset();
  builder->AddListColumn(std::nullopt, 1,
                         cobble::structured::ListConfig{.max_elements = 2});
  const auto schema = builder->Commit();
  COBBLE_CHECK(schema.Type("default", 1).kind ==
               cobble::structured::ColumnKind::kList);
}

template <typename Database> void VerifyPointApi(Database &db) {
  auto builder = db.UpdateSchema();
  builder.AddListColumn(
      std::nullopt, 1,
      cobble::structured::ListConfig{
          .max_elements = 3,
          .retain_mode = cobble::structured::ListRetainMode::kLast,
          .preserve_element_ttl = false});
  const auto schema = builder.Commit();
  bool consumed = false;
  try {
    (void)builder.Commit();
  } catch (const cobble::Error &error) {
    consumed = error.code() == cobble::ErrorCode::kInvalidState;
  }
  COBBLE_CHECK(consumed);
  COBBLE_CHECK(schema.Type("default", 0).kind ==
               cobble::structured::ColumnKind::kBytes);
  COBBLE_CHECK(schema.Type("default", 1).kind ==
               cobble::structured::ColumnKind::kList);

  db.PutBytes(0, Bytes("key"), 0, Bytes("base"));
  db.MergeBytes(0, Bytes("key"), 0, Bytes("-merged"));

  const std::array<std::string, 2> first = {"one", "two"};
  const std::array<cobble::BytesView, 2> first_views = {Bytes(first[0]),
                                                        Bytes(first[1])};
  db.PutList(0, Bytes("key"), 1, first_views);
  const std::array<cobble::BytesView, 2> second_views = {Bytes("three"),
                                                         Bytes("four")};
  db.MergeList(0, Bytes("key"), 1, second_views);

  auto row = db.Get(0, Bytes("key"));
  COBBLE_CHECK(row.Found());
  COBBLE_CHECK(row.ColumnCount() == 2);
  COBBLE_CHECK(row.Kind(0) == cobble::structured::ColumnKind::kBytes);
  COBBLE_CHECK(String(row.Bytes(0)) == "base-merged");
  COBBLE_CHECK(row.Kind(1) == cobble::structured::ColumnKind::kList);
  COBBLE_CHECK(row.ListSize(1) == 3);
  COBBLE_CHECK(String(row.ListElement(1, 0)) == "two");
  COBBLE_CHECK(String(row.ListElement(1, 1)) == "three");
  COBBLE_CHECK(String(row.ListElement(1, 2)) == "four");

  // The views point into Rust-owned Bytes and outlive every caller input.
  const auto retained = row.ListElement(1, 2);
  COBBLE_CHECK(String(retained) == "four");

  cobble::structured::ReadOptions projected;
  const std::array<std::size_t, 1> columns = {1};
  projected.SetColumns(columns);
  auto projected_row = db.Get(0, Bytes("key"), projected);
  COBBLE_CHECK(projected_row.ColumnCount() == 1);
  COBBLE_CHECK(projected_row.Kind(0) == cobble::structured::ColumnKind::kList);
  // Reuse the same native options to exercise the persistent projection cache.
  COBBLE_CHECK(db.Get(0, Bytes("key"), projected).ListSize(0) == 3);

  db.Delete(0, Bytes("key"), 0);
  row = db.Get(0, Bytes("key"));
  COBBLE_CHECK(!row.HasColumn(0));
  COBBLE_CHECK(row.HasColumn(1));
  COBBLE_CHECK(!db.Get(3, Bytes("missing")).Found());
}

void VerifySharded(const std::filesystem::path &root) {
  std::filesystem::create_directories(root);
  const auto config = Config(root);
  const auto config_path = root / "config.json";
  {
    std::ofstream output(config_path);
    output << config;
  }
  const std::array<cobble::BucketRange, 2> ranges = {cobble::BucketRange{0, 1},
                                                     cobble::BucketRange{2, 3}};
  auto db = cobble::structured::Db::Open(config, ranges);
  VerifyPointApi(db);
  db.SetTime(1234);
  COBBLE_CHECK(db.NowSeconds() == 1234);
  COBBLE_CHECK(!db.Id().empty());
  db.SwitchMemtableType(cobble::MemtableType::kSkiplist, true);
  (void)db.LoadReadonlyFilesToPrimary();
  COBBLE_CHECK(!db.Metrics().empty());

  auto pending = db.StartSnapshot();
  const auto pending_id = pending.id();
  const auto async_snapshot = pending.Wait();
  COBBLE_CHECK(async_snapshot.snapshot_id == pending_id);
  COBBLE_CHECK(db.GetShardSnapshot(pending_id).snapshot_id == pending_id);
  (void)db.RetainSnapshot(pending_id);

  db.PutBytes(1, Bytes("before-switch"), 0, Bytes("kept"));
  const auto exact = db.TakeSnapshot();
  db.PutBytes(1, Bytes("after-switch"), 0, Bytes("removed"));
  {
    auto active_builder = db.UpdateSchema();
    bool rejected = false;
    try {
      db.SwitchToSnapshot(exact.snapshot_id);
    } catch (const cobble::Error &error) {
      rejected = error.code() == cobble::ErrorCode::kInvalidState;
    }
    COBBLE_CHECK(rejected);
    (void)active_builder.Commit();
  }
  db.SwitchToSnapshot(exact.snapshot_id);
  COBBLE_CHECK(db.Get(1, Bytes("before-switch")).Found());
  COBBLE_CHECK(!db.Get(1, Bytes("after-switch")).Found());
  COBBLE_CHECK(db.Get(0, Bytes("key")).ListSize(1) == 3);

  auto cancelled = db.StartSnapshot();
  (void)db.CancelSnapshot(cancelled.id());
  (void)db.ExpireSnapshot(pending_id);
  db.PutBytes(1, Bytes("wal-tail"), 0, Bytes("latest"));
  const auto db_id = db.Id();
  db.Close();

  auto exact_resume = cobble::structured::Db::ResumeFromSnapshotFile(
      config_path.string(), exact.snapshot_id, db_id);
  COBBLE_CHECK(!exact_resume.Get(1, Bytes("wal-tail")).Found());
  exact_resume.Close();

  auto latest_resume =
      cobble::structured::Db::ResumeFile(config_path.string(), db_id);
  COBBLE_CHECK(latest_resume.Get(1, Bytes("wal-tail")).Found());
  latest_resume.Close();
}

void VerifySingle(const std::filesystem::path &root) {
  auto db = cobble::structured::SingleDb::Open(Config(root));
  VerifyPointApi(db);
  db.SetTime(2345);
  COBBLE_CHECK(db.NowSeconds() == 2345);
  db.SwitchMemtableType(cobble::MemtableType::kVec, true);
  (void)db.LoadReadonlyFilesToPrimary();
  auto pending = db.StartSnapshot();
  const auto pending_id = pending.id();
  const auto snapshot = pending.Wait();
  COBBLE_CHECK(snapshot.id == pending_id);
  COBBLE_CHECK(!db.ListSnapshots().empty());
  (void)db.RetainSnapshot(snapshot.id);
  const auto sync_snapshot = db.TakeSnapshot();
  COBBLE_CHECK(sync_snapshot.total_buckets == 4);
  (void)db.ExpireSnapshot(snapshot.id);
  db.Close();
}

void VerifyRescale(const std::filesystem::path &root) {
  const auto config = Config(root, 6);
  const std::array source_ranges = {cobble::BucketRange{2, 5}};
  const std::array target_ranges = {cobble::BucketRange{0, 1}};
  auto source = cobble::structured::Db::Open(config, source_ranges);
  auto target = cobble::structured::Db::Open(config, target_ranges);
  source.PutBytes(2, Bytes("rescale"), 0, Bytes("value"));
  const auto snapshot = source.TakeSnapshot();
  COBBLE_CHECK(source.RetainSnapshot(snapshot.snapshot_id));

  const std::array expanded = {cobble::BucketRange{2, 3}};
  (void)target.ExpandBucket(source.Id(), snapshot.snapshot_id, expanded,
                            cobble::ExpandStorageMode::kReferencePersistent);
  COBBLE_CHECK(String(target.Get(2, Bytes("rescale")).Bytes(0)) == "value");
  target.WaitForExpandAdoption(std::chrono::seconds(1));

  (void)target.ShrinkBucket(expanded);
  bool removed = false;
  try {
    removed = !target.Get(2, Bytes("rescale")).Found();
  } catch (const cobble::Error &) {
    removed = true;
  }
  COBBLE_CHECK(removed);
  target.Close();
  source.Close();
}

} // namespace

int main() {
  try {
    cobble_test::TempDirectory root("cobble-cpp-structured-basic");
    VerifyDetachedBuilder(root.path() / "detached");
    VerifySharded(root.path() / "sharded");
    VerifySingle(root.path() / "single");
    VerifyRescale(root.path() / "rescale");
    std::cout << "structured C++ basic capability test passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
