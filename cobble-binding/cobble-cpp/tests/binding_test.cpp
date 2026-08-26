#include <cobble/cobble.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

#define CHECK(condition)                                                     \
  do {                                                                       \
    if (!(condition)) {                                                      \
      throw std::runtime_error("check failed: " #condition);                \
    }                                                                        \
  } while (false)

cobble::BytesView Bytes(std::string_view value) {
  return {reinterpret_cast<const std::uint8_t*>(value.data()), value.size()};
}

std::string String(cobble::BytesView value) {
  return {reinterpret_cast<const char*>(value.data()), value.size()};
}

std::string FileUrl(const std::filesystem::path& path) {
  const auto generic = path.generic_string();
  return "file://" + std::string(generic.starts_with('/') ? "" : "/") +
         generic;
}

std::uint16_t U16(const std::uint8_t* value) {
  return static_cast<std::uint16_t>(value[0]) |
         (static_cast<std::uint16_t>(value[1]) << 8U);
}

std::uint32_t U32(const std::uint8_t* value) {
  std::uint32_t result = 0;
  for (std::size_t i = 0; i < 4; ++i) {
    result |= static_cast<std::uint32_t>(value[i]) << (i * 8U);
  }
  return result;
}

std::uint64_t U64(const std::uint8_t* value) {
  std::uint64_t result = 0;
  for (std::size_t i = 0; i < 8; ++i) {
    result |= static_cast<std::uint64_t>(value[i]) << (i * 8U);
  }
  return result;
}

void VerifyEncodedBatch(const std::vector<std::uint8_t>& encoded,
                        std::size_t expected_rows) {
  CHECK(encoded.size() >= 24);
  CHECK(std::memcmp(encoded.data(), "CBRB", 4) == 0);
  CHECK(U16(encoded.data() + 4) == 1);
  CHECK(U16(encoded.data() + 6) == 24);
  CHECK(U32(encoded.data() + 12) == expected_rows);
  CHECK(U64(encoded.data() + 16) == encoded.size());

  std::size_t offset = 24;
  for (std::size_t row = 0; row < expected_rows; ++row) {
    CHECK(offset + 12 <= encoded.size());
    CHECK(U16(encoded.data() + offset) == 0);
    const auto key_length = U32(encoded.data() + offset + 4);
    const auto column_count = U32(encoded.data() + offset + 8);
    offset += 12;
    CHECK(offset + key_length <= encoded.size());
    offset += key_length;
    for (std::size_t column = 0; column < column_count; ++column) {
      CHECK(offset + 8 <= encoded.size());
      const auto length = U64(encoded.data() + offset);
      offset += 8;
      if (length != std::numeric_limits<std::uint64_t>::max()) {
        CHECK(length <= encoded.size() - offset);
        offset += static_cast<std::size_t>(length);
      }
    }
  }
  CHECK(offset == encoded.size());
}

}  // namespace

int main() {
  const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
  const auto root = std::filesystem::temp_directory_path() /
                    ("cobble-cpp-binding-" + std::to_string(nonce));
  std::filesystem::remove_all(root);

  const std::string config =
      R"({"volumes":[{"base_dir":")" + FileUrl(root) +
      R"(","kinds":["meta","primary_data_priority_high"]}],"num_columns":2,"total_buckets":16,"block_cache_size":0})";

  {
    auto db = cobble::Database::Open(config);
    CHECK(!cobble::Version().empty());

    db.Put(0, Bytes("key-1"), 0, Bytes("value-1-0"));
    db.Put(0, Bytes("key-1"), 1, Bytes("value-1-1"));

    auto row = db.Get(0, Bytes("key-1"));
    CHECK(row.found());
    CHECK(row.column_count() == 2);
    CHECK(String(row.column(0)) == "value-1-0");
    CHECK(String(row.column(1)) == "value-1-1");

    auto missing = db.Get(0, Bytes("missing"));
    CHECK(!missing.found());

    cobble::ReadOptions one_column;
    one_column.columns = {1};
    std::array<std::uint8_t, 2> small = {0xAA, 0xBB};
    const auto too_small =
        db.GetColumnInto(0, Bytes("key-1"), small, one_column);
    CHECK(too_small.status == cobble::BufferStatus::kBufferTooSmall);
    CHECK(too_small.bytes_required == std::string_view("value-1-1").size());
    CHECK((small == std::array<std::uint8_t, 2>{0xAA, 0xBB}));

    std::vector<std::uint8_t> value(too_small.bytes_required);
    const auto copied =
        db.GetColumnInto(0, Bytes("key-1"), value, one_column);
    CHECK(copied.status == cobble::BufferStatus::kOk);
    CHECK(String(value) == "value-1-1");

    cobble::WriteBatch batch;
    batch.Put(0, Bytes("key-2"), 0, Bytes("value-2-0"));
    batch.Put(0, Bytes("key-3"), 0, Bytes("value-3-0"));
    batch.Delete(0, Bytes("key-1"), 0);
    CHECK(batch.size() == 3);
    db.Write(std::move(batch));

    auto scan = db.Scan(0, Bytes("key-1"), Bytes("key-4"));
    auto owned = scan.Next(16);
    CHECK(owned.row_count() == 3);
    CHECK(String(owned.key(0)) == "key-1");
    CHECK(!owned.has_column(0, 0));
    CHECK(String(owned.column(0, 1)) == "value-1-1");
    CHECK(String(owned.key(1)) == "key-2");
    CHECK(String(owned.column(1, 0)) == "value-2-0");
    CHECK(owned.end());

    auto encoded_scan = db.Scan(0, std::nullopt, std::nullopt);
    std::array<std::uint8_t, 1> tiny{};
    const auto pending = encoded_scan.NextBatchInto(16, tiny);
    CHECK(pending.status == cobble::BufferStatus::kBufferTooSmall);
    CHECK(pending.bytes_required > tiny.size());

    std::vector<std::uint8_t> encoded(pending.bytes_required);
    const auto encoded_result = encoded_scan.NextBatchInto(16, encoded);
    CHECK(encoded_result.status == cobble::BufferStatus::kOk);
    CHECK(encoded_result.bytes_written == encoded.size());
    CHECK(encoded_result.row_count == 3);
    VerifyEncodedBatch(encoded, 3);

    const auto snapshot = db.Snapshot();
    bool materialized = false;
    for (int attempt = 0; attempt < 200 && !materialized; ++attempt) {
      const auto snapshots = db.ListSnapshots();
      materialized =
          std::find(snapshots.begin(), snapshots.end(), snapshot) !=
          snapshots.end();
      if (!materialized) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
    CHECK(materialized);
    CHECK(db.SnapshotManifestJson(snapshot).find("\"id\":") !=
           std::string::npos);
  }

  bool saw_config_error = false;
  try {
    (void)cobble::Database::Open(R"({"total_buckets":0})");
  } catch (const cobble::Error& error) {
    saw_config_error = error.code() == cobble::ErrorCode::kConfiguration;
  }
  CHECK(saw_config_error);

  std::filesystem::remove_all(root);
}
