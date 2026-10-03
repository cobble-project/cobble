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

template <typename Database, typename Put, typename Get>
void VerifyChangedRoot(const std::filesystem::path& root, Put put, Get get) {
  const auto old_root = root / "old";
  const auto new_root = root / "new";
  const std::array source_ranges = {cobble::BucketRange{2, 3}};
  const std::array target_ranges = {cobble::BucketRange{0, 1}};
  const auto target_config = Config(new_root, old_root);
  auto source = Database::Open(Config(old_root), source_ranges);
  put(source);
  const auto snapshot = source.TakeSnapshot();
  COBBLE_CHECK(source.RetainSnapshot(snapshot.snapshot_id));

  const std::array modes = {
      cobble::ExpandStorageMode::kAdoptAsync,
      cobble::ExpandStorageMode::kReferencePersistent,
      cobble::ExpandStorageMode::kReferencePersistentWithCache};
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
        mode == cobble::ExpandStorageMode::kReferencePersistent
            ? std::optional<std::span<const cobble::BucketRange>>{}
            : std::optional<std::span<const cobble::BucketRange>>{
                  source_ranges};
    (void)target.ExpandBucketFromManifest(source.Id(), snapshot.manifest_path,
                                          ranges, mode);
    target.WaitForExpandAdoption(std::chrono::seconds(10));
    COBBLE_CHECK(get(target));
    const auto imported = target.TakeSnapshot();
    COBBLE_CHECK(imported.manifest_path.starts_with(
        cobble_test::FileUrl(new_root) + "/"));
    const auto id = target.Id();
    target.Close();
    auto resumed =
        Database::ResumeFromSnapshot(target_config, imported.snapshot_id, id);
    COBBLE_CHECK(get(resumed));
    resumed.Close();
  }
  source.Close();
}

}  // namespace

int main() {
  try {
    cobble_test::TempDirectory root("cobble-cpp-rescale-manifest");
    VerifyChangedRoot<cobble::Db>(
        root.path() / "raw",
        [](auto& db) {
          db.Put(2, cobble_test::Bytes("moved"), 0,
                 cobble_test::Bytes("value"));
          db.SwitchMemtableType(cobble::MemtableType::kSkiplist, true);
          db.Put(2, cobble_test::Bytes("active"), 0,
                 cobble_test::Bytes("tail"));
        },
        [](auto& db) {
          return cobble_test::String(
                     db.Get(2, cobble_test::Bytes("moved")).Column(0)) ==
                     "value" &&
                 cobble_test::String(
                     db.Get(2, cobble_test::Bytes("active")).Column(0)) ==
                     "tail";
        });
    VerifyChangedRoot<cobble::structured::Db>(
        root.path() / "structured",
        [](auto& db) {
          db.PutBytes(2, cobble_test::Bytes("moved"), 0,
                      cobble_test::Bytes("value"));
          db.SwitchMemtableType(cobble::MemtableType::kSkiplist, true);
          db.PutBytes(2, cobble_test::Bytes("active"), 0,
                      cobble_test::Bytes("tail"));
        },
        [](auto& db) {
          return cobble_test::String(
                     db.Get(2, cobble_test::Bytes("moved")).Bytes(0)) ==
                     "value" &&
                 cobble_test::String(
                     db.Get(2, cobble_test::Bytes("active")).Bytes(0)) ==
                     "tail";
        });
    std::cout << "raw and structured changed-root manifest import passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
