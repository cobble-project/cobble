#include "test_support.hpp"

#include <cobble/coordinator.hpp>
#include <cobble/structured.hpp>

#include <array>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;
using cobble_test::String;

constexpr std::array<cobble::Byte, 2> kFirst = {0x00, 0x00};
constexpr std::array<cobble::Byte, 2> kMiddle = {0x00, 0xff};
constexpr std::array<cobble::Byte, 2> kLast = {0xff, 0x00};
constexpr std::array<cobble::Byte, 2> kEnd = {0xff, 0x01};
constexpr std::array<cobble::Byte, 2> kOutside = {0xff, 0x02};

std::string Config(const std::filesystem::path& root) {
  return R"({"volumes":[{"base_dir":")" + FileUrl(root) +
         R"(","kinds":["meta","primary_data_priority_high","snapshot"]}],"num_columns":1,"total_buckets":4,"memtable_capacity":"8KB","base_file_size":"16KB","block_cache_size":0,"snapshot_retention":20,"wal_enabled":false})";
}

void WriteFile(const std::filesystem::path& path, std::string_view contents) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  COBBLE_CHECK(output.is_open());
  output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  output.close();
  COBBLE_CHECK(output.good());
}

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  COBBLE_CHECK(input.is_open());
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}

void WriteRaw(const std::filesystem::path& root) {
  const auto config_path = root / "raw-config.json";
  const auto config = Config(root / "raw-db");
  WriteFile(config_path, config);
  const std::array left_range = {cobble::BucketRange{0, 1}};
  const std::array right_range = {cobble::BucketRange{2, 3}};
  auto left = cobble::Db::Open(config, left_range);
  auto right = cobble::Db::Open(config, right_range);
  left.Put(0, kFirst, 0, Bytes("raw-first"));
  left.Put(1, kMiddle, 0, Bytes("raw-middle"));
  right.Put(2, kLast, 0, Bytes("raw-last"));
  right.Put(3, kOutside, 0, Bytes("raw-outside"));
  WriteFile(root / "raw-left.json", left.TakeSnapshot().ToJson());
  WriteFile(root / "raw-right.json", right.TakeSnapshot().ToJson());
  left.Close();
  right.Close();
}

void WriteStructured(const std::filesystem::path& root) {
  const auto config_path = root / "structured-config.json";
  const auto config = Config(root / "structured-db");
  WriteFile(config_path, config);
  const std::array left_range = {cobble::BucketRange{0, 1}};
  const std::array right_range = {cobble::BucketRange{2, 3}};
  auto left = cobble::structured::Db::Open(config, left_range);
  auto right = cobble::structured::Db::Open(config, right_range);
  for (auto* db : {&left, &right}) {
    auto schema = db->UpdateSchema();
    schema.AddListColumn(std::nullopt, 1, {});
    (void)schema.Commit();
  }
  left.PutBytes(0, kFirst, 0, Bytes("structured-first"));
  const std::array<cobble::BytesView, 2> elements = {Bytes("one"),
                                                     Bytes("two")};
  left.PutList(0, kFirst, 1, elements);
  left.PutBytes(1, kMiddle, 0, Bytes("structured-middle"));
  right.PutBytes(2, kLast, 0, Bytes("structured-last"));
  right.PutBytes(3, kOutside, 0, Bytes("structured-outside"));
  WriteFile(root / "structured-left.json", left.TakeSnapshot().ToJson());
  WriteFile(root / "structured-right.json", right.TakeSnapshot().ToJson());
  left.Close();
  right.Close();
}

void Coordinate(const std::filesystem::path& root, std::string_view name) {
  const std::string prefix(name);
  const auto config_path = root / (prefix + "-config.json");
  const std::array shards = {
      cobble::ShardSnapshot::FromJson(ReadFile(root / (prefix + "-left.json"))),
      cobble::ShardSnapshot::FromJson(
          ReadFile(root / (prefix + "-right.json")))};
  COBBLE_CHECK(shards[0].has_schema_metadata);
  COBBLE_CHECK(shards[1].has_schema_metadata);
  auto coordinator = cobble::DbCoordinator::OpenFile(config_path.string());
  const auto global = coordinator.MaterializeGlobalSnapshot(4, 100, shards);
  WriteFile(root / (prefix + "-global.json"), global.ToJson());
  if (name == "raw") {
    auto plan = cobble::ScanPlan::FromGlobalSnapshot(global);
    plan.WithStart(kFirst).WithEnd(kEnd);
    WriteFile(root / "raw-plan.json", plan.ToJson());
  } else {
    auto plan = cobble::structured::ScanPlan::FromGlobalSnapshot(global);
    plan.WithStart(kFirst).WithEnd(kEnd);
    WriteFile(root / "structured-plan.json", plan.ToJson());
  }
}

using ScanRow = std::pair<cobble::BucketId, std::vector<cobble::Byte>>;

std::vector<cobble::Byte> KeyBytes(cobble::BytesView key) {
  return {key.begin(), key.end()};
}

void ReadRaw(const std::filesystem::path& root) {
  const auto config_path = root / "raw-config.json";
  const auto global =
      cobble::GlobalSnapshot::FromJson(ReadFile(root / "raw-global.json"));
  auto reader = cobble::Reader::OpenFile(config_path.string(), global);
  COBBLE_CHECK(String(reader.Get(0, kFirst).Column(0)) == "raw-first");
  COBBLE_CHECK(String(reader.Get(2, kLast).Column(0)) == "raw-last");
  COBBLE_CHECK(String(reader.Get(3, kOutside).Column(0)) == "raw-outside");
  const auto splits =
      cobble::ScanPlan::FromJson(ReadFile(root / "raw-plan.json")).Splits();
  COBBLE_CHECK(splits.size() == 2);
  std::vector<ScanRow> rows;
  constexpr std::array<std::string_view, 3> values = {"raw-first", "raw-middle",
                                                      "raw-last"};
  for (const auto& split : splits) {
    auto cursor = split.OpenScannerFile(config_path.string());
    while (true) {
      auto batch = cursor.Next(2);
      for (std::size_t row = 0; row < batch.RowCount(); ++row) {
        COBBLE_CHECK(batch.Bucket(row) < values.size());
        COBBLE_CHECK(String(batch.Column(row, 0)) == values[batch.Bucket(row)]);
        const auto key = batch.Key(row);
        rows.emplace_back(batch.Bucket(row), KeyBytes(key));
      }
      if (batch.End()) break;
    }
  }
  COBBLE_CHECK((rows == std::vector<ScanRow>{{0, KeyBytes(kFirst)},
                                             {1, KeyBytes(kMiddle)},
                                             {2, KeyBytes(kLast)}}));
}

void ReadStructured(const std::filesystem::path& root) {
  const auto config_path = root / "structured-config.json";
  const auto global = cobble::GlobalSnapshot::FromJson(
      ReadFile(root / "structured-global.json"));
  auto reader =
      cobble::structured::Reader::OpenFile(config_path.string(), global);
  auto first = reader.Get(0, kFirst);
  COBBLE_CHECK(String(first.Bytes(0)) == "structured-first");
  COBBLE_CHECK(first.ListSize(1) == 2);
  COBBLE_CHECK(String(first.ListElement(1, 1)) == "two");
  COBBLE_CHECK(String(reader.Get(2, kLast).Bytes(0)) == "structured-last");
  COBBLE_CHECK(String(reader.Get(3, kOutside).Bytes(0)) ==
               "structured-outside");
  const auto splits = cobble::structured::ScanPlan::FromJson(
                          ReadFile(root / "structured-plan.json"))
                          .Splits();
  COBBLE_CHECK(splits.size() == 2);
  std::vector<ScanRow> rows;
  constexpr std::array<std::string_view, 3> values = {
      "structured-first", "structured-middle", "structured-last"};
  for (const auto& split : splits) {
    auto cursor = split.OpenScannerFile(config_path.string());
    while (true) {
      auto batch = cursor.Next(2);
      for (std::size_t row = 0; row < batch.RowCount(); ++row) {
        COBBLE_CHECK(batch.Bucket(row) < values.size());
        COBBLE_CHECK(String(batch.Bytes(row, 0)) == values[batch.Bucket(row)]);
        const auto key = batch.Key(row);
        rows.emplace_back(batch.Bucket(row), KeyBytes(key));
      }
      if (batch.End()) break;
    }
  }
  COBBLE_CHECK((rows == std::vector<ScanRow>{{0, KeyBytes(kFirst)},
                                             {1, KeyBytes(kMiddle)},
                                             {2, KeyBytes(kLast)}}));
}

}  // namespace

int main(int argc, char** argv) {
  try {
    COBBLE_CHECK(argc == 3);
    const std::string_view phase(argv[1]);
    const std::filesystem::path root(argv[2]);
    if (phase == "write") {
      std::filesystem::create_directories(root);
      WriteRaw(root);
      WriteStructured(root);
    } else if (phase == "coordinate") {
      Coordinate(root, "raw");
      Coordinate(root, "structured");
    } else if (phase == "read") {
      ReadRaw(root);
      ReadStructured(root);
    } else {
      throw std::runtime_error("unknown transfer test phase");
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
