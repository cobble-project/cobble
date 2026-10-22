#include "test_support.hpp"

#include <cobble/coordinator.hpp>

#include <array>
#include <filesystem>
#include <iostream>
#include <string>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;

std::string Volume(const std::filesystem::path& root) {
  return R"({"base_dir":")" + FileUrl(root) +
         R"(","kinds":["meta","primary_data_priority_high","snapshot"],"access_id":"test-access","secret_key":"test-secret","custom_options":{}})";
}

std::string Config(const std::filesystem::path& root, bool incremental = true) {
  return R"({"volumes":[)" + Volume(root) +
         R"(],"num_columns":1,"total_buckets":1,"memtable_capacity":"4MB","l0_file_limit":100,"snapshot_on_flush":false,"snapshot_only_track":true,"value_separation_threshold":"16B","snapshot_disable_incremental_base_link":)" +
         (incremental ? "false}" : "true}");
}

std::filesystem::path Manifest(const std::filesystem::path& root,
                               const cobble::ShardSnapshot& snapshot) {
  return root / snapshot.db_id / "snapshot" /
         ("SNAPSHOT-" + std::to_string(snapshot.snapshot_id));
}

cobble::ShardSnapshot Snapshot(cobble::Db& db, std::string_view key) {
  db.Put(0, Bytes(key), 0, Bytes(std::string(128, 'v')));
  return db.TakeSnapshot();
}

void ShardCleanup(const std::filesystem::path& root) {
  auto db = cobble::Db::Open(Config(root));
  const auto base = Snapshot(db, "base");
  const auto next = Snapshot(db, "next");
  db.Close();
  const auto preserved = cobble::CleanupShardSnapshot(
      Volume(root), "[]", base.db_id, base.snapshot_id);
  COBBLE_CHECK(preserved.deleted_files == 0);
  COBBLE_CHECK(preserved.deleted_shard_snapshots == 0);
  COBBLE_CHECK(preserved.deleted_global_snapshots == 0);
  COBBLE_CHECK(preserved.preserved_snapshots.size() == 1);
  const auto& identity = preserved.preserved_snapshots.front();
  COBBLE_CHECK(identity.db_id == base.db_id);
  COBBLE_CHECK(identity.snapshot_id == base.snapshot_id);
  COBBLE_CHECK(identity.manifest_path == base.manifest_path);
  COBBLE_CHECK(std::filesystem::exists(Manifest(root, base)));

  const auto forced = cobble::CleanupShardSnapshot(
      Volume(root), "[]", base.db_id, base.snapshot_id, {false, false});
  COBBLE_CHECK(forced.deleted_shard_snapshots == 1);
  COBBLE_CHECK(forced.deleted_files > 0);
  COBBLE_CHECK(forced.preserved_snapshots.empty());
  COBBLE_CHECK(!std::filesystem::exists(Manifest(root, base)));
  COBBLE_CHECK(std::filesystem::exists(Manifest(root, next)));
}

void GlobalCleanup(const std::filesystem::path& root) {
  const auto metadata = root / "metadata";
  const auto shards_root = root / "shards";
  std::array<cobble::ShardSnapshot, 3> shards;
  {
    auto db = cobble::Db::Open(Config(shards_root, false));
    auto coordinator = cobble::DbCoordinator::Open(Config(metadata));
    for (std::size_t index = 0; index < shards.size(); ++index) {
      shards[index] = Snapshot(db, std::to_string(index));
      const std::array input = {shards[index]};
      (void)coordinator.MaterializeGlobalSnapshot(1, 7 + index, input);
    }
    db.Close();
  }
  COBBLE_CHECK(shards.back().snapshot_id < 8);
  const auto result = cobble::CleanupGlobalSnapshot(
      Volume(metadata), "[" + Volume(shards_root) + "]", 8, {true, true});
  COBBLE_CHECK(result.deleted_global_snapshots == 2);
  COBBLE_CHECK(result.deleted_shard_snapshots == 2);
  COBBLE_CHECK(result.preserved_snapshots.empty());
  for (std::size_t index = 0; index < shards.size(); ++index) {
    COBBLE_CHECK(std::filesystem::exists(
                     Manifest(shards_root, shards[index])) == (index == 2));
    COBBLE_CHECK(
        std::filesystem::exists(metadata / "snapshot" /
                                ("SNAPSHOT-" + std::to_string(index + 7))) ==
        (index == 2));
  }
  const auto reader_config = R"({"volumes":[)" + Volume(metadata) + "," +
                             Volume(shards_root) +
                             R"(],"num_columns":1,"total_buckets":1})";
  auto reader = cobble::Reader::OpenCurrent(reader_config);
  COBBLE_CHECK(reader.Get(0, Bytes("2")).Found());
}

void ExportCleanupAndErrors(const std::filesystem::path& root) {
  const auto source = root / "source";
  const auto standalone = root / "standalone";
  const auto exported = root / "exported";
  cobble::ShardSnapshot original;
  cobble::ShardSnapshot shard;
  cobble::ShardSnapshot global_shard;
  {
    auto db = cobble::Db::Open(Config(source));
    auto coordinator = cobble::DbCoordinator::Open(Config(source));
    original = Snapshot(db, "export");
    shard =
        db.ExportSnapshot(original.snapshot_id, Volume(standalone / "shards"))
            .Wait();
    global_shard =
        db.ExportSnapshot(original.snapshot_id, Volume(exported / "shards"))
            .Wait();
    const std::array input = {global_shard};
    coordinator.MaterializeExportedGlobalSnapshot(
        coordinator.TakeExportedGlobalSnapshot(1, input), Volume(exported));
    db.Close();
  }
  const auto shard_result = cobble::CleanupShardSnapshot(
      Volume(standalone / "shards"), "[]", shard.db_id, shard.snapshot_id);
  COBBLE_CHECK(shard_result.deleted_shard_snapshots == 1);
  COBBLE_CHECK(shard_result.deleted_global_snapshots == 0);
  COBBLE_CHECK(shard_result.deleted_files > 0);
  const auto global_result =
      cobble::CleanupGlobalSnapshot(Volume(exported), "[]", 0);
  COBBLE_CHECK(global_result.deleted_shard_snapshots == 1);
  COBBLE_CHECK(global_result.deleted_global_snapshots == 1);
  COBBLE_CHECK(global_result.deleted_files > 0);
  COBBLE_CHECK(
      !std::filesystem::exists(Manifest(standalone / "shards", shard)));
  COBBLE_CHECK(
      !std::filesystem::exists(Manifest(exported / "shards", global_shard)));
  COBBLE_CHECK(std::filesystem::exists(Manifest(source, original)));
  COBBLE_CHECK(!std::filesystem::exists(exported / "snapshot/SNAPSHOT-0"));

  for (const auto bad : {"{", "null"}) {
    bool failed = false;
    try {
      (void)cobble::CleanupGlobalSnapshot(bad, "[]", 0);
    } catch (const cobble::Error& error) {
      failed = error.Code() == cobble::ErrorCode::kInput;
    }
    COBBLE_CHECK(failed);
  }
  for (const auto bad : {"{", "null", "[null]"}) {
    bool failed = false;
    try {
      (void)cobble::CleanupGlobalSnapshot(Volume(source), bad, 0);
    } catch (const cobble::Error& error) {
      failed = error.Code() == cobble::ErrorCode::kInput;
    }
    COBBLE_CHECK(failed);
  }
  bool failed = false;
  try {
    (void)cobble::CleanupShardSnapshot(Volume(source), "[]", "../escape", 0);
  } catch (const cobble::Error& error) {
    failed = error.Code() == cobble::ErrorCode::kConfiguration;
  }
  COBBLE_CHECK(failed);
}

}  // namespace

int main() {
  try {
    cobble_test::TempDirectory temporary("cobble-cpp-cleanup");
    ShardCleanup(temporary.path() / "incremental");
    GlobalCleanup(temporary.path() / "global");
    ExportCleanupAndErrors(temporary.path() / "exports");
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
