#include "test_support.hpp"

#include <cobble/structured.hpp>

#include <array>
#include <iostream>
#include <optional>
#include <string>

namespace {

std::string Config(const std::filesystem::path& root,
                   const std::optional<std::filesystem::path>& source = {}) {
  std::string volumes =
      R"({"base_dir":")" + cobble_test::FileUrl(root) +
      R"(","kinds":["meta","primary_data_priority_high","snapshot"]})";
  if (source) {
    volumes += R"(,{"base_dir":")" + cobble_test::FileUrl(*source) +
               R"(","kinds":["readonly"]})";
  }
  return R"({"volumes":[)" + volumes +
         R"(],"num_columns":1,"total_buckets":4,"memtable_capacity":"8KB","base_file_size":"16KB","block_cache_size":0,"wal_enabled":false})";
}

void CheckSnapshotRange(const cobble::ShardSnapshot& snapshot,
                        cobble::BucketId end) {
  COBBLE_CHECK(snapshot.ranges.size() == 1);
  COBBLE_CHECK(snapshot.ranges.front().start_inclusive == 0);
  COBBLE_CHECK(snapshot.ranges.front().end_inclusive == end);
}

template <typename Database, typename Put, typename Get>
void VerifySnapshotOptions(std::string_view config, const Database& source,
                           const cobble::ShardSnapshot& snapshot,
                           std::span<const cobble::BucketRange> source_ranges,
                           std::span<const cobble::BucketRange> target_ranges,
                           Put put, Get get) {
  auto target = Database::Open(config, target_ranges);
  put(target, 0);
  constexpr auto mode = cobble::ImportStorageMode::kReferencePersistent;
  const std::array options = {
      cobble::RescaleOptions{false, false}, cobble::RescaleOptions{true, false},
      cobble::RescaleOptions{false, true}, cobble::RescaleOptions{}};
  for (const bool from_manifest : {false, true}) {
    for (const auto option : options) {
      const auto snapshot_count =
          option.snapshot_before + option.snapshot_after;
      const auto before = target.TakeSnapshot().snapshot_id;
      const auto imported =
          from_manifest ? target.ExpandBucketFromManifest(
                              source.Id(), snapshot.manifest_path,
                              source_ranges, mode, option)
                        : target.ExpandBucket(source.Id(), snapshot.snapshot_id,
                                              source_ranges, mode, option);
      COBBLE_CHECK(imported == snapshot.snapshot_id);
      COBBLE_CHECK(get(target, 2));
      COBBLE_CHECK(get(target, 0));
      const auto expanded = target.TakeSnapshot();
      COBBLE_CHECK(expanded.snapshot_id == before + snapshot_count + 1);
      CheckSnapshotRange(expanded, 3);
      if (option.snapshot_before) {
        CheckSnapshotRange(target.GetShardSnapshot(before + 1), 1);
      }
      if (option.snapshot_after) {
        CheckSnapshotRange(target.GetShardSnapshot(expanded.snapshot_id - 1),
                           3);
      }

      const auto pre_shrink = target.ShrinkBucket(source_ranges, option);
      COBBLE_CHECK(pre_shrink.has_value() == option.snapshot_before);
      if (pre_shrink) {
        COBBLE_CHECK(*pre_shrink == expanded.snapshot_id + 1);
        CheckSnapshotRange(target.GetShardSnapshot(*pre_shrink), 3);
      }
      COBBLE_CHECK(!target.Get(2, cobble_test::Bytes("moved")).Found());
      COBBLE_CHECK(!target.Get(2, cobble_test::Bytes("active")).Found());
      COBBLE_CHECK(get(target, 0));
      const auto shrunk = target.TakeSnapshot();
      COBBLE_CHECK(shrunk.snapshot_id ==
                   expanded.snapshot_id + snapshot_count + 1);
      CheckSnapshotRange(shrunk, 1);
      if (option.snapshot_after) {
        CheckSnapshotRange(target.GetShardSnapshot(shrunk.snapshot_id - 1), 1);
      }
    }
  }
  target.Close();
}

template <typename Database, typename Put, typename Get>
void VerifyChangedRoot(const std::filesystem::path& root, Put put, Get get) {
  const auto old_root = root / "old";
  const auto new_root = root / "new";
  const std::array source_ranges = {cobble::BucketRange{2, 3}};
  const std::array target_ranges = {cobble::BucketRange{0, 1}};
  const auto target_config = Config(new_root, old_root);
  auto source = Database::Open(Config(old_root), source_ranges);
  put(source, 2);
  const auto snapshot = source.TakeSnapshot();
  COBBLE_CHECK(source.RetainSnapshot(snapshot.snapshot_id));

  const std::array modes = {
      cobble::ImportStorageMode::kAdoptAsync,
      cobble::ImportStorageMode::kReferencePersistent,
      cobble::ImportStorageMode::kReferencePersistentWithCache};
  for (const auto mode : modes) {
    auto target = Database::Open(target_config, target_ranges);
    bool invalid_manifest = false;
    try {
      (void)target.ExpandBucketFromManifest(
          source.Id(), snapshot.manifest_path + "?secret=not-persisted",
          source_ranges, mode);
    } catch (const cobble::Error& error) {
      invalid_manifest = error.Code() == cobble::ErrorCode::kConfiguration;
    }
    COBBLE_CHECK(invalid_manifest);
    bool invalid_ranges = false;
    try {
      (void)target.ExpandBucketFromManifest(
          source.Id(), snapshot.manifest_path,
          std::span<const cobble::BucketRange>{}, mode);
    } catch (const cobble::Error& error) {
      invalid_ranges = error.Code() == cobble::ErrorCode::kInput;
    }
    COBBLE_CHECK(invalid_ranges);

    // Exercise both the inferred and explicit source ranges.
    const auto ranges =
        mode == cobble::ImportStorageMode::kReferencePersistent
            ? std::optional<std::span<const cobble::BucketRange>>{}
            : std::optional<std::span<const cobble::BucketRange>>{
                  source_ranges};
    (void)target.ExpandBucketFromManifest(source.Id(), snapshot.manifest_path,
                                          ranges, mode);
    target.WaitForImportAdoption(std::chrono::seconds(10));
    COBBLE_CHECK(get(target, 2));
    const auto imported = target.TakeSnapshot();
    COBBLE_CHECK(imported.manifest_path.starts_with(
        cobble_test::FileUrl(new_root) + "/"));
    const auto id = target.Id();
    target.Close();
    auto resumed =
        Database::ResumeFromSnapshot(target_config, imported.snapshot_id, id);
    COBBLE_CHECK(get(resumed, 2));
    resumed.Close();
    const std::array imported_ranges = {cobble::BucketRange{2, 2}};
    for (bool from_manifest : {false, true}) {
      auto fresh =
          from_manifest
              ? Database::RestoreNewFromManifest(target_config,
                                                 snapshot.manifest_path,
                                                 imported_ranges, mode)
              : Database::RestoreNew(Config(old_root), snapshot.snapshot_id,
                                     source.Id(), imported_ranges, mode);
      COBBLE_CHECK(fresh.Id() != source.Id());
      fresh.WaitForImportAdoption(std::chrono::seconds(10));
      COBBLE_CHECK(get(fresh, 2));
      fresh.Close();
    }
  }
  VerifySnapshotOptions(Config(old_root), source, snapshot, source_ranges,
                        target_ranges, put, get);
  source.Close();
}

}  // namespace

int main() {
  try {
    cobble_test::TempDirectory root("cobble-cpp-rescale-manifest");
    VerifyChangedRoot<cobble::Db>(
        root.path() / "raw",
        [](auto& db, cobble::BucketId bucket) {
          db.Put(bucket, cobble_test::Bytes("moved"), 0,
                 cobble_test::Bytes("value"));
          db.SwitchMemtableType(cobble::MemtableType::kSkiplist, true);
          db.Put(bucket, cobble_test::Bytes("active"), 0,
                 cobble_test::Bytes("tail"));
        },
        [](auto& db, cobble::BucketId bucket) {
          return cobble_test::String(
                     db.Get(bucket, cobble_test::Bytes("moved")).Column(0)) ==
                     "value" &&
                 cobble_test::String(
                     db.Get(bucket, cobble_test::Bytes("active")).Column(0)) ==
                     "tail";
        });
    VerifyChangedRoot<cobble::structured::Db>(
        root.path() / "structured",
        [](auto& db, cobble::BucketId bucket) {
          db.PutBytes(bucket, cobble_test::Bytes("moved"), 0,
                      cobble_test::Bytes("value"));
          db.SwitchMemtableType(cobble::MemtableType::kSkiplist, true);
          db.PutBytes(bucket, cobble_test::Bytes("active"), 0,
                      cobble_test::Bytes("tail"));
        },
        [](auto& db, cobble::BucketId bucket) {
          return cobble_test::String(
                     db.Get(bucket, cobble_test::Bytes("moved")).Bytes(0)) ==
                     "value" &&
                 cobble_test::String(
                     db.Get(bucket, cobble_test::Bytes("active")).Bytes(0)) ==
                     "tail";
        });
    std::cout
        << "raw and structured manifest import and rescale options passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
