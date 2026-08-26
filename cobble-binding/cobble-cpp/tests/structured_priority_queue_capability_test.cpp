#include "test_support.hpp"

#include <cobble/structured.hpp>

#include <array>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace {

using cobble_test::Bytes;
using cobble_test::FileUrl;
using cobble_test::String;

std::string Config(const std::filesystem::path &root,
                   std::size_t total_buckets) {
  const auto url = FileUrl(root);
  return R"({"volumes":[{"base_dir":")" + url +
         R"(","kinds":["meta","primary_data_priority_high","snapshot"]},{"base_dir":")" +
         url + R"(","kinds":["wal"]}],"num_columns":1,"total_buckets":)" +
         std::to_string(total_buckets) +
         R"(,"memtable_capacity":"4KB","base_file_size":"8KB","block_size":"512B","block_cache_size":0,"wal_enabled":true,"wal_flush_interval_ms":5})";
}

template <typename Callback> void ExpectInvalidState(Callback callback) {
  bool rejected = false;
  try {
    callback();
  } catch (const cobble::Error &error) {
    rejected = error.code() == cobble::ErrorCode::kInvalidState;
  }
  COBBLE_CHECK(rejected);
}

void CheckEntry(const cobble::structured::OwnedPriorityQueueEntry &entry,
                std::string_view key, std::string_view value) {
  COBBLE_CHECK(String(entry.Key()) == key);
  COBBLE_CHECK(String(entry.Value()) == value);
}

std::string CursorString(const cobble::structured::PriorityQueue &queue,
                         cobble::BucketId bucket) {
  const auto cursor = queue.Cursor(bucket);
  COBBLE_CHECK(cursor.has_value());
  return String(cobble::BytesView(cursor->data(), cursor->size()));
}

template <typename Database>
void VerifyBuilderExcludesQueueCreation(Database &db) {
  {
    auto queue = db.NewPriorityQueue("builder-existing");
    COBBLE_CHECK(queue.ColumnFamily() == "builder-existing");
  }
  {
    auto builder = db.UpdateSchema();
    builder.AddBytesColumn("builder-family", 0);
    {
      auto queue = db.GetPriorityQueue("builder-existing");
      COBBLE_CHECK(queue.ColumnFamily() == "builder-existing");
    }
    ExpectInvalidState(
        [&] { (void)db.NewPriorityQueue("after-builder-new"); });
    ExpectInvalidState(
        [&] { (void)db.GetOrNewPriorityQueue("after-builder-get-or-new"); });
    (void)builder.Commit();
  }
  {
    auto queue = db.NewPriorityQueue("after-builder-new");
    COBBLE_CHECK(queue.ColumnFamily() == "after-builder-new");
  }
  {
    auto queue = db.GetOrNewPriorityQueue("after-builder-get-or-new");
    COBBLE_CHECK(queue.ColumnFamily() == "after-builder-get-or-new");
  }
}

template <typename Database>
void VerifyEmptyCursor(Database &db, cobble::BucketId bucket) {
  auto queue = db.NewPriorityQueue("empty-cursor");
  queue.Advance(bucket, cobble::BytesView{});
  const auto cursor = queue.Cursor(bucket);
  COBBLE_CHECK(cursor.has_value());
  COBBLE_CHECK(cursor->empty());
}

template <typename Database>
void VerifyCommonQueueApi(Database &db, cobble::BucketId bucket) {
  {
    auto queue = db.NewPriorityQueue("jobs");
    COBBLE_CHECK(queue.ColumnFamily() == "jobs");
  }
  ExpectInvalidState([&] { (void)db.NewPriorityQueue("jobs"); });
  ExpectInvalidState([&] { (void)db.GetPriorityQueue("missing"); });

  auto queue = db.GetOrNewPriorityQueue("jobs");
  ExpectInvalidState(
      [&] { (void)db.NewPriorityQueue("while-queue-active"); });
  ExpectInvalidState(
      [&] { (void)db.GetOrNewPriorityQueue("while-queue-active"); });
  queue.Offer(bucket, Bytes("k2"), Bytes("v2"));
  queue.Offer(bucket, Bytes("k1"), Bytes("left"));
  queue.Offer(bucket, Bytes("k1"), Bytes("-right"));
  queue.Offer(bucket, Bytes("deleted"), Bytes("gone"));
  queue.Delete(bucket, Bytes("deleted"));

  {
    const auto peek = queue.Peek(bucket);
    COBBLE_CHECK(peek.has_value());
    CheckEntry(*peek, "k1", "left-right");
    COBBLE_CHECK(!queue.Cursor(bucket).has_value());
  }
  {
    const auto polled = queue.Poll(bucket);
    COBBLE_CHECK(polled.has_value());
    CheckEntry(*polled, "k1", "left-right");
  }
  COBBLE_CHECK(CursorString(queue, bucket) == "k1");

  queue.Offer(bucket, Bytes("k3"), Bytes("v3"));
  queue.Offer(bucket, Bytes("k4"), Bytes("v4"));
  const auto peeked = queue.PeekBatch(bucket, 2);
  COBBLE_CHECK(peeked.Size() == 2);
  COBBLE_CHECK(String(peeked.Entry(0).key) == "k2");
  COBBLE_CHECK(String(peeked.Entry(1).key) == "k3");
  COBBLE_CHECK(CursorString(queue, bucket) == "k1");

  const auto cursor_before_zero = *queue.Cursor(bucket);
  COBBLE_CHECK(queue.PollBatch(bucket, 0).Size() == 0);
  COBBLE_CHECK(*queue.Cursor(bucket) == cursor_before_zero);

  std::array<std::uint8_t, 4> too_small = {0xa5, 0xa5, 0xa5, 0xa5};
  const auto before = too_small;
  const auto small = queue.PollBatchInto(bucket, too_small, 2);
  COBBLE_CHECK(small.status == cobble::BufferStatus::kBufferTooSmall);
  COBBLE_CHECK(too_small == before);
  ExpectInvalidState([&] { (void)queue.Peek(bucket); });
  ExpectInvalidState([&] {
    std::vector<std::uint8_t> wrong(small.bytes_required);
    (void)queue.PollBatchInto(bucket, wrong, 1);
  });

  std::vector<std::uint8_t> encoded(small.bytes_required);
  const auto success = queue.PollBatchInto(bucket, encoded, 2);
  COBBLE_CHECK(success.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(success.row_count == 2);
  COBBLE_CHECK(std::memcmp(encoded.data(), "CSRB", 4) == 0);
  COBBLE_CHECK(CursorString(queue, bucket) == "k3");

  std::array<std::uint8_t, 1> peek_small = {0x5a};
  const auto peek_required = queue.PeekInto(bucket, peek_small);
  COBBLE_CHECK(peek_required.status ==
               cobble::BufferStatus::kBufferTooSmall);
  COBBLE_CHECK(peek_small[0] == 0x5a);
  std::vector<std::uint8_t> peek_encoded(peek_required.bytes_required);
  const auto peek_success = queue.PeekInto(bucket, peek_encoded);
  COBBLE_CHECK(peek_success.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(CursorString(queue, bucket) == "k3");

  const auto remaining = queue.Peek(bucket);
  COBBLE_CHECK(remaining.has_value());
  CheckEntry(*remaining, "k4", "v4");

  const auto boundary = queue.PeekBatch(bucket, std::nullopt);
  COBBLE_CHECK(boundary.Size() >= 1);
  queue.Advance(bucket, Bytes("k4"));
  COBBLE_CHECK(!queue.Peek(bucket).has_value());

  std::array<std::uint8_t, 24> empty_encoded{};
  const auto empty = queue.PollBatchInto(bucket, empty_encoded, 0);
  COBBLE_CHECK(empty.status == cobble::BufferStatus::kEnd);
  COBBLE_CHECK(empty.row_count == 0);
  COBBLE_CHECK(CursorString(queue, bucket) == "k4");

  queue.Offer(bucket, Bytes("k5"), Bytes("v5"));
  std::array<std::uint8_t, 1> poll_small = {0x33};
  const auto poll_required = queue.PollInto(bucket, poll_small);
  COBBLE_CHECK(poll_required.status ==
               cobble::BufferStatus::kBufferTooSmall);
  std::vector<std::uint8_t> poll_encoded(poll_required.bytes_required);
  const auto poll_success = queue.PollInto(bucket, poll_encoded);
  COBBLE_CHECK(poll_success.status == cobble::BufferStatus::kOk);
  COBBLE_CHECK(CursorString(queue, bucket) == "k5");
  COBBLE_CHECK(!queue.Peek(bucket).has_value());

  // Flush enough rows to multiple SST blocks and verify nullopt consumes one
  // physical-boundary batch at a time without gaps or duplicates.
  constexpr std::size_t boundary_rows = 128;
  const std::string boundary_value(128, 'x');
  for (std::size_t index = 0; index < boundary_rows; ++index) {
    const auto key = "z" + std::to_string(1000 + index);
    queue.Offer(bucket, Bytes(key), Bytes(boundary_value));
  }
  db.SwitchMemtableType(cobble::MemtableType::kVec, true);
  const auto first_boundary = queue.PeekBatch(bucket, std::nullopt);
  COBBLE_CHECK(first_boundary.Size() > 0);
  COBBLE_CHECK(first_boundary.Size() < boundary_rows);

  std::size_t consumed = 0;
  std::string last_key;
  while (true) {
    const auto batch = queue.PollBatch(bucket, std::nullopt);
    if (batch.Size() == 0)
      break;
    for (std::size_t index = 0; index < batch.Size(); ++index) {
      const auto key = String(batch.Entry(index).key);
      COBBLE_CHECK(last_key.empty() || last_key < key);
      last_key = key;
      ++consumed;
    }
  }
  COBBLE_CHECK(consumed == boundary_rows);
}

void VerifyResume(const std::filesystem::path &root) {
  const auto config = Config(root, 2);
  auto db = cobble::structured::Db::Open(config);
  const auto db_id = db.Id();
  {
    auto queue = db.NewPriorityQueue("resume-jobs");
    queue.Offer(1, Bytes("k1"), Bytes("v1"));
    queue.Offer(1, Bytes("k2"), Bytes("v2"));
    CheckEntry(*queue.Poll(1), "k1", "v1");
    COBBLE_CHECK(CursorString(queue, 1) == "k1");
  }
  (void)db.TakeSnapshot();
  db.Close();

  auto resumed = cobble::structured::Db::Resume(config, db_id);
  auto queue = resumed.GetPriorityQueue("resume-jobs");
  COBBLE_CHECK(CursorString(queue, 1) == "k1");
  CheckEntry(*queue.Poll(1), "k2", "v2");
  queue = resumed.GetPriorityQueue("resume-jobs");
  COBBLE_CHECK(!queue.Peek(1).has_value());
  resumed.Close();
}

void VerifySharded(const std::filesystem::path &root) {
  const auto config = Config(root, 4);
  auto db = cobble::structured::Db::Open(config);

  // A normal structured family must not be accepted as a priority queue.
  {
    auto builder = db.UpdateSchema();
    (void)builder.AddBytesColumn("plain", 0).Commit();
  }
  VerifyBuilderExcludesQueueCreation(db);
  ExpectInvalidState([&] { (void)db.GetPriorityQueue("plain"); });
  VerifyCommonQueueApi(db, 3);
  VerifyEmptyCursor(db, 2);

  // Queue ownership participates in the existing exclusive-owner protocol.
  {
    std::optional<cobble::structured::PriorityQueue> queue(
        db.GetPriorityQueue("jobs"));
    auto builder = db.UpdateSchema();
    builder.AddBytesColumn(std::nullopt, 1);
    ExpectInvalidState([&] { (void)builder.Commit(); });
    // Releasing the queue makes the same staged builder retryable.
    queue.reset();
    (void)builder.Commit();
    ExpectInvalidState([&] { (void)builder.Commit(); });
  }

  // Rust-owned entries do not retain the queue and preserve their views.
  std::optional<cobble::structured::OwnedPriorityQueueEntry> owned;
  {
    auto queue = db.GetPriorityQueue("jobs");
    queue.Offer(0, Bytes("owned"), Bytes("payload"));
    owned = queue.Peek(0);
  }
  COBBLE_CHECK(owned.has_value());
  CheckEntry(*owned, "owned", "payload");

  db.SwitchMemtableType(cobble::MemtableType::kVec, true);
  const auto snapshot = db.TakeSnapshot();
  {
    auto queue = db.GetPriorityQueue("jobs");
    ExpectInvalidState([&] { db.SwitchToSnapshot(snapshot.snapshot_id); });
  }
  db.SwitchToSnapshot(snapshot.snapshot_id);

  // Destroying the public parent wrapper does not invalidate a queue child.
  auto parent = std::make_unique<cobble::structured::Db>(
      cobble::structured::Db::Open(Config(root / "detached", 4)));
  auto detached = parent->NewPriorityQueue("detached-jobs");
  parent.reset();
  detached.Offer(2, Bytes("key"), Bytes("value"));
  CheckEntry(*detached.Peek(2), "key", "value");

  // Explicit close is visible to existing children.
  auto close_db = cobble::structured::Db::Open(Config(root / "closed", 4));
  auto queue = close_db.NewPriorityQueue("closed-jobs");
  close_db.Close();
  ExpectInvalidState([&] {
    queue.Offer(0, Bytes("closed"), Bytes("value"));
  });
  db.Close();
}

void VerifySingle(const std::filesystem::path &root) {
  auto db = cobble::structured::SingleDb::Open(Config(root, 2));
  VerifyBuilderExcludesQueueCreation(db);
  VerifyCommonQueueApi(db, 1);
  VerifyEmptyCursor(db, 0);

  auto queue = db.GetPriorityQueue("jobs");
  db.Close();
  ExpectInvalidState([&] { (void)queue.Peek(1); });
}

} // namespace

int main() {
  try {
    cobble_test::TempDirectory root("cobble-cpp-structured-priority-queue");
    VerifySharded(root.path() / "sharded");
    VerifySingle(root.path() / "single");
    VerifyResume(root.path() / "resume");
    std::cout << "structured priority queue capability test passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "structured priority queue capability test failed: "
              << error.what() << '\n';
    return 1;
  }
}
