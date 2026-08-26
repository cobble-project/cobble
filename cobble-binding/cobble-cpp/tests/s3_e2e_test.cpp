#include "test_support.hpp"

#include <cobble/structured.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>

namespace {

using cobble_test::Bytes;
using cobble_test::String;

constexpr std::size_t kRowCount = 512;
constexpr std::size_t kValueBytes = 512;
constexpr std::size_t kStructuredRowCount = 384;
constexpr std::size_t kStructuredValueBytes = 384;

std::string Key(std::size_t row) {
  std::ostringstream stream;
  stream << "s3-key-" << std::setw(6) << std::setfill('0') << row;
  return stream.str();
}

std::string Value(std::size_t row) {
  std::string value = "row=" + std::to_string(row) + ";";
  value.resize(kValueBytes, static_cast<char>('a' + (row % 26)));
  return value;
}

std::string StructuredKey(std::size_t row) {
  std::ostringstream stream;
  stream << "structured-key-" << std::setw(6) << std::setfill('0') << row;
  return stream.str();
}

std::string StructuredValue(std::size_t row) {
  std::string value = "structured-row=" + std::to_string(row) + ";";
  value.resize(kStructuredValueBytes,
               static_cast<char>('A' + (row % 26)));
  return value;
}

std::array<std::string, 3> StructuredList(std::size_t row) {
  return {"group=" + std::to_string(row % 17),
          "row=" + std::to_string(row),
          std::string(48, static_cast<char>('0' + (row % 10)))};
}

std::optional<std::string> Environment(const char* name) {
  if (const char* value = std::getenv(name); value != nullptr && *value != '\0') {
    return value;
  }
  return std::nullopt;
}

std::string JsonString(std::string_view value) {
  std::string result;
  result.reserve(value.size() + 2);
  result.push_back('"');
  for (const unsigned char character : value) {
    switch (character) {
      case '"':
        result += "\\\"";
        break;
      case '\\':
        result += "\\\\";
        break;
      case '\b':
        result += "\\b";
        break;
      case '\f':
        result += "\\f";
        break;
      case '\n':
        result += "\\n";
        break;
      case '\r':
        result += "\\r";
        break;
      case '\t':
        result += "\\t";
        break;
      default:
        if (character < 0x20) {
          constexpr char kHex[] = "0123456789abcdef";
          result += "\\u00";
          result.push_back(kHex[character >> 4]);
          result.push_back(kHex[character & 0x0f]);
        } else {
          result.push_back(static_cast<char>(character));
        }
    }
  }
  result.push_back('"');
  return result;
}

std::string S3BaseDir(std::string endpoint, std::string_view bucket,
                      std::string_view prefix) {
  const auto scheme_end = endpoint.find("://");
  COBBLE_CHECK(scheme_end != std::string::npos);
  const auto scheme = endpoint.substr(0, scheme_end);
  COBBLE_CHECK(scheme == "http" || scheme == "https");
  endpoint.erase(0, scheme_end + 3);
  while (!endpoint.empty() && endpoint.back() == '/') {
    endpoint.pop_back();
  }
  COBBLE_CHECK(!endpoint.empty());
  return "s3://" + endpoint + "/" + std::string(bucket) + "/" +
         std::string(prefix) +
         "?endpoint_scheme=" + scheme +
         "&region=us-east-1&disable_config_load=true"
         "&disable_ec2_metadata=true&enable_virtual_host_style=false";
}

std::string ConfigJson(std::string_view endpoint, std::string_view bucket,
                       std::string_view access_id, std::string_view secret_key,
                       std::string_view prefix) {
  const auto base_dir = S3BaseDir(std::string(endpoint), bucket, prefix);
  return R"({"volumes":[{"base_dir":)" + JsonString(base_dir) +
         R"(,"access_id":)" + JsonString(access_id) + R"(,"secret_key":)" +
         JsonString(secret_key) +
         R"(,"kinds":["meta","primary_data_priority_high","snapshot"]}],"num_columns":1,"total_buckets":1,"block_cache_size":0})";
}

cobble::SnapshotId WaitForSnapshot(const cobble::Database& db,
                                   cobble::SnapshotId snapshot) {
  for (std::size_t attempt = 0; attempt < 3'000; ++attempt) {
    const auto snapshots = db.ListSnapshots();
    if (std::find(snapshots.begin(), snapshots.end(), snapshot) !=
        snapshots.end()) {
      COBBLE_CHECK(db.SnapshotManifestJson(snapshot).find("\"id\":") !=
                   std::string::npos);
      return snapshot;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  throw std::runtime_error("snapshot did not materialize within 30 seconds");
}

void VerifyRawS3(std::string_view endpoint, std::string_view bucket,
                 std::string_view access_id, std::string_view secret_key,
                 std::string_view prefix) {
  const auto config =
      ConfigJson(endpoint, bucket, access_id, secret_key, prefix);
  auto db = cobble::Database::Open(config);
  cobble::WriteBatch batch;
  for (std::size_t row = 0; row < kRowCount; ++row) {
    const auto key = Key(row);
    const auto value = Value(row);
    batch.Put(0, Bytes(key), 0, Bytes(value));
    if (batch.size() == 64) {
      db.Write(std::move(batch));
      batch = cobble::WriteBatch();
    }
  }
  COBBLE_CHECK(batch.size() == 0);
  const auto snapshot = WaitForSnapshot(db, db.Snapshot());
  db.Close();

  auto resumed = cobble::Database::Resume(config, snapshot);
  for (std::size_t row = 0; row < kRowCount; row += 31) {
    const auto key = Key(row);
    const auto result = resumed.Get(0, Bytes(key));
    COBBLE_CHECK(result.found());
    COBBLE_CHECK(result.column_count() == 1);
    COBBLE_CHECK(String(result.column(0)) == Value(row));
  }

  {
    auto scan = resumed.Scan(0, std::nullopt, std::nullopt);
    std::size_t expected = 0;
    while (true) {
      const auto rows = scan.Next(73);
      for (std::size_t index = 0; index < rows.row_count(); ++index) {
        COBBLE_CHECK(String(rows.key(index)) == Key(expected));
        COBBLE_CHECK(String(rows.column(index, 0)) == Value(expected));
        ++expected;
      }
      if (rows.end()) {
        break;
      }
    }
    COBBLE_CHECK(expected == kRowCount);
  }
  resumed.Close();
}

void VerifyStructuredS3(std::string_view endpoint, std::string_view bucket,
                        std::string_view access_id,
                        std::string_view secret_key,
                        std::string_view prefix) {
  const auto config =
      ConfigJson(endpoint, bucket, access_id, secret_key, prefix);
  auto db = cobble::structured::Db::Open(config);
  {
    auto schema = db.UpdateSchema();
    schema.AddListColumn(
        std::nullopt, 1,
        cobble::structured::ListConfig{
            .max_elements = 8,
            .retain_mode = cobble::structured::ListRetainMode::kLast,
            .preserve_element_ttl = false});
    (void)schema.Commit();
  }

  cobble::structured::WriteBatch batch;
  for (std::size_t row = 0; row < kStructuredRowCount; ++row) {
    const auto key = StructuredKey(row);
    const auto value = StructuredValue(row);
    const auto elements = StructuredList(row);
    const std::array<cobble::BytesView, 3> element_views = {
        Bytes(elements[0]), Bytes(elements[1]), Bytes(elements[2])};
    batch.PutBytes(0, Bytes(key), 0, Bytes(value));
    batch.PutList(0, Bytes(key), 1, element_views);
    if (batch.size() == 96) {
      db.Write(batch);
    }
  }
  COBBLE_CHECK(batch.empty());

  const auto db_id = db.Id();
  const auto snapshot = db.TakeSnapshot();
  db.Close();

  auto resumed = cobble::structured::Db::ResumeFromSnapshot(
      config, snapshot.snapshot_id, db_id,
      cobble::RecoveryMode::kSnapshotOnly);
  for (std::size_t row = 0; row < kStructuredRowCount; row += 29) {
    const auto result = resumed.Get(0, Bytes(StructuredKey(row)));
    COBBLE_CHECK(result.Found());
    COBBLE_CHECK(result.ColumnCount() == 2);
    COBBLE_CHECK(String(result.Bytes(0)) == StructuredValue(row));
    const auto expected_list = StructuredList(row);
    COBBLE_CHECK(result.ListSize(1) == expected_list.size());
    for (std::size_t element = 0; element < expected_list.size(); ++element) {
      COBBLE_CHECK(String(result.ListElement(1, element)) ==
                   expected_list[element]);
    }
  }

  {
    auto scan = resumed.Scan(0);
    std::size_t expected = 0;
    while (true) {
      const auto rows = scan.Next(61);
      for (std::size_t index = 0; index < rows.RowCount(); ++index) {
        COBBLE_CHECK(String(rows.Key(index)) == StructuredKey(expected));
        COBBLE_CHECK(rows.ColumnCount(index) == 2);
        COBBLE_CHECK(String(rows.Bytes(index, 0)) ==
                     StructuredValue(expected));
        const auto expected_list = StructuredList(expected);
        COBBLE_CHECK(rows.ListSize(index, 1) == expected_list.size());
        for (std::size_t element = 0; element < expected_list.size();
             ++element) {
          COBBLE_CHECK(String(rows.ListElement(index, 1, element)) ==
                       expected_list[element]);
        }
        ++expected;
      }
      if (rows.End()) {
        break;
      }
    }
    COBBLE_CHECK(expected == kStructuredRowCount);
  }
  resumed.Close();
}

}  // namespace

int main() {
  try {
    const auto endpoint = Environment("COBBLE_S3_ENDPOINT");
    const auto bucket = Environment("COBBLE_S3_BUCKET");
    const auto access_id = Environment("COBBLE_S3_ACCESS_ID");
    const auto secret_key = Environment("COBBLE_S3_SECRET_KEY");
    if (!endpoint || !bucket || !access_id || !secret_key) {
      std::cout << "skipping C++ S3 E2E test: COBBLE_S3_* variables are not fully set\n";
      return 0;
    }

    const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto base_prefix =
        "cobble-cpp-s3-e2e-" + std::to_string(nonce);
    VerifyRawS3(*endpoint, *bucket, *access_id, *secret_key,
                base_prefix + "/raw");
    VerifyStructuredS3(*endpoint, *bucket, *access_id, *secret_key,
                       base_prefix + "/structured");
    std::cout << "verified " << kRowCount << " raw rows and "
              << kStructuredRowCount
              << " structured BYTES+LIST rows through C++ S3 exact snapshot "
                 "resume and ordered scans\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "C++ S3 end-to-end test failed: " << error.what() << '\n';
    return 1;
  }
}
