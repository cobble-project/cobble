#include "test_support.hpp"

#include <algorithm>
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
    const auto config = ConfigJson(*endpoint, *bucket, *access_id, *secret_key,
                                   "cobble-cpp-s3-e2e-" + std::to_string(nonce));
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
    std::cout << "verified " << kRowCount
              << " rows through C++ S3 snapshot-close-resume-read\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "C++ S3 end-to-end test failed: " << error.what() << '\n';
    return 1;
  }
}
