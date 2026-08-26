#include <cobble/cobble.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

constexpr std::size_t kRowCount = 20'000;
constexpr std::size_t kBucketCount = 16;
constexpr std::size_t kRowsPerWriteBatch = 250;
constexpr std::size_t kRowsPerScanBatch = 257;
constexpr std::size_t kColumnZeroBytes = 256;
constexpr std::size_t kColumnOneBytes = 384;

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

std::string Key(std::size_t row) {
  std::ostringstream stream;
  stream << "key-" << std::setw(8) << std::setfill('0') << row;
  return stream.str();
}

std::string Value(std::size_t row, std::size_t column, std::size_t size) {
  const auto prefix = "row=" + std::to_string(row) +
                      ";column=" + std::to_string(column) + ";";
  std::string value = prefix;
  value.resize(size, static_cast<char>('a' + ((row + column) % 26)));
  return value;
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

class TempDatabaseDirectory {
 public:
  TempDatabaseDirectory() {
    const auto nonce =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("cobble-cpp-bulk-e2e-" + std::to_string(nonce));
    std::filesystem::remove_all(path_);
  }

  ~TempDatabaseDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

void WriteRows(const cobble::Database& db) {
  cobble::WriteBatch batch;
  std::size_t rows_in_batch = 0;
  for (std::size_t row = 0; row < kRowCount; ++row) {
    const auto bucket = static_cast<cobble::BucketId>(row % kBucketCount);
    const auto key = Key(row);
    const auto column_zero = Value(row, 0, kColumnZeroBytes);
    const auto column_one = Value(row, 1, kColumnOneBytes);
    batch.Put(bucket, Bytes(key), 0, Bytes(column_zero));
    batch.Put(bucket, Bytes(key), 1, Bytes(column_one));
    ++rows_in_batch;

    if (rows_in_batch == kRowsPerWriteBatch) {
      CHECK(batch.size() == kRowsPerWriteBatch * 2);
      db.Write(std::move(batch));
      batch = cobble::WriteBatch();
      rows_in_batch = 0;
    }
  }
  CHECK(rows_in_batch == 0);
}

void VerifyPointReads(const cobble::Database& db) {
  for (std::size_t row = 0; row < kRowCount; row += 7) {
    const auto bucket = static_cast<cobble::BucketId>(row % kBucketCount);
    const auto key = Key(row);
    const auto result = db.Get(bucket, Bytes(key));
    CHECK(result.found());
    CHECK(result.column_count() == 2);
    CHECK(String(result.column(0)) == Value(row, 0, kColumnZeroBytes));
    CHECK(String(result.column(1)) == Value(row, 1, kColumnOneBytes));
  }

  const auto missing_key = Key(kRowCount + 1);
  CHECK(!db.Get(0, Bytes(missing_key)).found());
}

void VerifyOwnedScans(const cobble::Database& db) {
  std::size_t total_rows = 0;
  for (std::size_t bucket = 0; bucket < kBucketCount; ++bucket) {
    auto scan = db.Scan(static_cast<cobble::BucketId>(bucket), std::nullopt,
                        std::nullopt);
    std::size_t expected_row = bucket;
    bool end = false;
    while (!end) {
      const auto batch = scan.Next(kRowsPerScanBatch);
      CHECK(batch.row_count() != 0 || batch.end());
      for (std::size_t index = 0; index < batch.row_count(); ++index) {
        CHECK(expected_row < kRowCount);
        CHECK(batch.bucket(index) == bucket);
        CHECK(String(batch.key(index)) == Key(expected_row));
        CHECK(batch.column_count(index) == 2);
        CHECK(batch.has_column(index, 0));
        CHECK(batch.has_column(index, 1));
        CHECK(String(batch.column(index, 0)) ==
              Value(expected_row, 0, kColumnZeroBytes));
        CHECK(String(batch.column(index, 1)) ==
              Value(expected_row, 1, kColumnOneBytes));
        expected_row += kBucketCount;
        ++total_rows;
      }
      end = batch.end();
    }
    CHECK(expected_row >= kRowCount);
  }
  CHECK(total_rows == kRowCount);
}

void VerifyEncodedRows(cobble::BytesView encoded, std::size_t bucket,
                       std::size_t& expected_row) {
  CHECK(encoded.size() >= 24);
  CHECK(std::memcmp(encoded.data(), "CBRB", 4) == 0);
  CHECK(U16(encoded.data() + 4) == 1);
  CHECK(U16(encoded.data() + 6) == 24);
  CHECK(U64(encoded.data() + 16) == encoded.size());

  const auto row_count = U32(encoded.data() + 12);
  std::size_t offset = 24;
  for (std::size_t index = 0; index < row_count; ++index) {
    CHECK(offset + 12 <= encoded.size());
    CHECK(U16(encoded.data() + offset) == bucket);
    CHECK(U16(encoded.data() + offset + 2) == 0);
    const auto key_size = U32(encoded.data() + offset + 4);
    const auto column_count = U32(encoded.data() + offset + 8);
    offset += 12;
    CHECK(column_count == 2);
    CHECK(offset + key_size <= encoded.size());
    CHECK(String(encoded.subspan(offset, key_size)) == Key(expected_row));
    offset += key_size;

    for (std::size_t column = 0; column < column_count; ++column) {
      CHECK(offset + 8 <= encoded.size());
      const auto value_size = U64(encoded.data() + offset);
      offset += 8;
      CHECK(value_size != std::numeric_limits<std::uint64_t>::max());
      CHECK(value_size <= encoded.size() - offset);
      const auto expected_size =
          column == 0 ? kColumnZeroBytes : kColumnOneBytes;
      CHECK(String(encoded.subspan(offset, static_cast<std::size_t>(value_size))) ==
            Value(expected_row, column, expected_size));
      offset += static_cast<std::size_t>(value_size);
    }
    expected_row += kBucketCount;
  }
  CHECK(offset == encoded.size());
}

void VerifyCallerOwnedScan(const cobble::Database& db) {
  constexpr cobble::BucketId bucket = 0;
  auto scan = db.Scan(bucket, std::nullopt, std::nullopt);
  std::vector<std::uint8_t> output(1);
  std::size_t expected_row = bucket;

  while (true) {
    const auto result = scan.NextBatchInto(113, output);
    if (result.status == cobble::BufferStatus::kBufferTooSmall) {
      CHECK(result.bytes_written == 0);
      CHECK(result.bytes_required > output.size());
      output.resize(result.bytes_required);
      continue;
    }

    CHECK(result.status == cobble::BufferStatus::kOk ||
          result.status == cobble::BufferStatus::kEnd);
    CHECK(result.bytes_written <= output.size());
    if (result.bytes_written != 0) {
      const cobble::BytesView encoded(output.data(), result.bytes_written);
      CHECK(U32(encoded.data() + 12) == result.row_count);
      VerifyEncodedRows(encoded, bucket, expected_row);
    }
    if (result.status == cobble::BufferStatus::kEnd) {
      break;
    }
  }
  CHECK(expected_row >= kRowCount);
}

cobble::SnapshotId WaitForSnapshot(const cobble::Database& db) {
  const auto snapshot = db.Snapshot();
  for (std::size_t attempt = 0; attempt < 3'000; ++attempt) {
    const auto snapshots = db.ListSnapshots();
    if (std::find(snapshots.begin(), snapshots.end(), snapshot) !=
        snapshots.end()) {
      CHECK(db.SnapshotManifestJson(snapshot).find("\"id\":") !=
            std::string::npos);
      return snapshot;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  throw std::runtime_error("snapshot did not materialize within 30 seconds");
}

}  // namespace

int main() {
  try {
    TempDatabaseDirectory directory;
    const std::string config =
        R"({"volumes":[{"base_dir":")" + FileUrl(directory.path()) +
        R"(","kinds":["meta","primary_data_priority_high"]}],"num_columns":2,"total_buckets":16,"memtable_capacity":"512KB","block_cache_size":0})";

    const auto started = std::chrono::steady_clock::now();
    cobble::SnapshotId snapshot = 0;
    {
      auto db = cobble::Database::Open(config);
      WriteRows(db);
      VerifyPointReads(db);
      VerifyOwnedScans(db);
      VerifyCallerOwnedScan(db);
      snapshot = WaitForSnapshot(db);
      db.Close();
    }

    {
      auto resumed = cobble::Database::Resume(config, snapshot);
      VerifyPointReads(resumed);
      VerifyOwnedScans(resumed);
      resumed.Close();
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started);
    const auto payload_bytes =
        kRowCount * (kColumnZeroBytes + kColumnOneBytes);
    std::cout << "verified " << kRowCount << " rows and " << payload_bytes
              << " value bytes across " << kBucketCount << " buckets in "
              << elapsed.count() << " ms\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "bulk C++ end-to-end test failed: " << error.what() << '\n';
    return 1;
  }
}
