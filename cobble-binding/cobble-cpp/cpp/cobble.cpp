#include <cobble/cobble.hpp>

#include <algorithm>
#include <limits>
#include <utility>

#include "rust/cxx.h"
#include "lib.rs.h"

namespace cobble {
namespace {

const Byte* NonNullData(BytesView value) noexcept {
  static constexpr Byte kEmpty = 0;
  return value.empty() ? &kEmpty : value.data();
}

Byte* NonNullData(MutableBytesView value) noexcept {
  static Byte empty = 0;
  return value.empty() ? &empty : value.data();
}

const char* NonNullData(std::string_view value) noexcept {
  static constexpr char kEmpty = '\0';
  return value.empty() ? &kEmpty : value.data();
}

rust::Slice<const Byte> RustBytes(BytesView value) noexcept {
  return {NonNullData(value), value.size()};
}

rust::Slice<Byte> RustBytes(MutableBytesView value) noexcept {
  return {NonNullData(value), value.size()};
}

rust::Str RustStr(std::string_view value) noexcept {
  return {NonNullData(value), value.size()};
}

rust::String RustString(std::string_view value) {
  return {NonNullData(value), value.size()};
}

ErrorCode ParseErrorCode(std::string_view message) noexcept {
  struct Prefix {
    std::string_view value;
    ErrorCode code;
  };
  static constexpr Prefix prefixes[] = {
      {"CB_URL:", ErrorCode::kUrl},
      {"CB_FILE_SYSTEM:", ErrorCode::kFileSystem},
      {"CB_IO:", ErrorCode::kIo},
      {"CB_MEMTABLE_FULL:", ErrorCode::kMemtableFull},
      {"CB_CONFIGURATION:", ErrorCode::kConfiguration},
      {"CB_INPUT:", ErrorCode::kInput},
      {"CB_COORDINATION:", ErrorCode::kCoordination},
      {"CB_INVALID_STATE:", ErrorCode::kInvalidState},
      {"CB_FILE_FORMAT:", ErrorCode::kFileFormat},
      {"CB_CHECKSUM:", ErrorCode::kChecksum},
      {"CB_CANCELLED:", ErrorCode::kCancelled},
  };
  for (const auto& prefix : prefixes) {
    if (message.starts_with(prefix.value)) {
      return prefix.code;
    }
  }
  return ErrorCode::kUnknown;
}

[[noreturn]] void ThrowTranslated(const rust::Error& error) {
  std::string message(error.what());
  throw Error(ParseErrorCode(message), std::move(message));
}

template <typename Function>
decltype(auto) Translate(Function&& function) {
  try {
    return std::forward<Function>(function)();
  } catch (const rust::Error& error) {
    ThrowTranslated(error);
  }
}

ffi::NativeReadOptions ToNative(const ReadOptions& options) {
  ffi::NativeReadOptions native;
  if (options.column_family) {
    native.column_family = RustString(*options.column_family);
  }
  native.columns.reserve(options.columns.size());
  for (const auto column : options.columns) {
    native.columns.push_back(static_cast<std::uint64_t>(column));
  }
  return native;
}

ffi::NativeWriteOptions ToNative(const WriteOptions& options) {
  ffi::NativeWriteOptions native;
  native.has_ttl_seconds = options.ttl_seconds.has_value();
  native.ttl_seconds = options.ttl_seconds.value_or(0);
  if (options.column_family) {
    native.column_family = RustString(*options.column_family);
  }
  native.await_durable = options.await_durable;
  return native;
}

ffi::NativeScanOptions ToNative(const ScanOptions& options) {
  if (options.read_ahead_bytes > std::numeric_limits<std::uint64_t>::max()) {
    throw Error(ErrorCode::kInput,
                "scan read_ahead_bytes exceeds the supported size");
  }
  ffi::NativeScanOptions native;
  if (options.column_family) {
    native.column_family = RustString(*options.column_family);
  }
  native.columns.reserve(options.columns.size());
  for (const auto column : options.columns) {
    native.columns.push_back(static_cast<std::uint64_t>(column));
  }
  native.read_ahead_bytes = static_cast<std::uint64_t>(options.read_ahead_bytes);
  native.has_max_rows = options.max_rows.has_value();
  native.max_rows = options.max_rows.value_or(0);
  native.preload_scan_cursor_block = options.preload_scan_cursor_block;
  native.stop_at_block_boundary = options.stop_at_block_boundary;
  return native;
}

BufferStatus ToBufferStatus(std::uint8_t status) {
  switch (status) {
    case 0:
      return BufferStatus::kOk;
    case 1:
      return BufferStatus::kNotFound;
    case 2:
      return BufferStatus::kEnd;
    case 3:
      return BufferStatus::kBufferTooSmall;
    case 4:
      return BufferStatus::kBlockBoundary;
    default:
      throw Error(ErrorCode::kInvalidState,
                  "Rust bridge returned an unknown buffer status");
  }
}

std::size_t ToSize(std::uint64_t value, std::string_view field) {
  if (value > std::numeric_limits<std::size_t>::max()) {
    throw Error(ErrorCode::kInvalidState,
                std::string(field) + " exceeds the C++ address space");
  }
  return static_cast<std::size_t>(value);
}

BufferResult ToBufferResult(const ffi::NativeBufferResult& native) {
  return {
      ToBufferStatus(native.status),
      ToSize(native.bytes_written, "bytes_written"),
      ToSize(native.bytes_required, "bytes_required"),
      ToSize(native.row_count, "row_count"),
  };
}

BytesView ToView(rust::Slice<const Byte> value) noexcept {
  return {value.data(), value.size()};
}

}  // namespace

Error::Error(ErrorCode code, std::string message)
    : std::runtime_error(std::move(message)), code_(code) {}

ErrorCode Error::code() const noexcept { return code_; }

struct OwnedRow::Impl {
  explicit Impl(rust::Box<ffi::NativeRow> native_row)
      : native(std::move(native_row)) {}
  rust::Box<ffi::NativeRow> native;
};

OwnedRow::OwnedRow(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedRow::OwnedRow(OwnedRow&&) noexcept = default;
OwnedRow& OwnedRow::operator=(OwnedRow&&) noexcept = default;
OwnedRow::~OwnedRow() = default;

bool OwnedRow::found() const noexcept {
  return impl_ && ffi::native_row_found(*impl_->native);
}

std::size_t OwnedRow::column_count() const noexcept {
  if (!impl_) {
    return 0;
  }
  const auto count = ffi::native_row_column_count(*impl_->native);
  return count > std::numeric_limits<std::size_t>::max()
             ? std::numeric_limits<std::size_t>::max()
             : static_cast<std::size_t>(count);
}

bool OwnedRow::has_column(std::size_t column) const {
  return impl_ && ffi::native_row_has_column(*impl_->native, column);
}

BytesView OwnedRow::column(std::size_t column) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedRow has been moved from");
  }
  return Translate([&] {
    return ToView(ffi::native_row_column(*impl_->native, column));
  });
}

struct OwnedBatch::Impl {
  explicit Impl(rust::Box<ffi::NativeBatch> native_batch)
      : native(std::move(native_batch)) {}
  rust::Box<ffi::NativeBatch> native;
};

OwnedBatch::OwnedBatch(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
OwnedBatch::OwnedBatch(OwnedBatch&&) noexcept = default;
OwnedBatch& OwnedBatch::operator=(OwnedBatch&&) noexcept = default;
OwnedBatch::~OwnedBatch() = default;

std::size_t OwnedBatch::row_count() const noexcept {
  if (!impl_) {
    return 0;
  }
  const auto count = ffi::native_batch_row_count(*impl_->native);
  return count > std::numeric_limits<std::size_t>::max()
             ? std::numeric_limits<std::size_t>::max()
             : static_cast<std::size_t>(count);
}

bool OwnedBatch::end() const noexcept {
  return impl_ && ffi::native_batch_end(*impl_->native);
}

bool OwnedBatch::stopped_at_block_boundary() const noexcept {
  return impl_ &&
         ffi::native_batch_stopped_at_block_boundary(*impl_->native);
}

BucketId OwnedBatch::bucket(std::size_t row) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  }
  return Translate(
      [&] { return ffi::native_batch_bucket(*impl_->native, row); });
}

BytesView OwnedBatch::key(std::size_t row) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  }
  return Translate(
      [&] { return ToView(ffi::native_batch_key(*impl_->native, row)); });
}

std::size_t OwnedBatch::column_count(std::size_t row) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  }
  return ToSize(Translate([&] {
                  return ffi::native_batch_column_count(*impl_->native, row);
                }),
                "column_count");
}

bool OwnedBatch::has_column(std::size_t row, std::size_t column) const {
  return impl_ &&
         ffi::native_batch_has_column(*impl_->native, row, column);
}

BytesView OwnedBatch::column(std::size_t row, std::size_t column) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "OwnedBatch has been moved from");
  }
  return Translate([&] {
    return ToView(ffi::native_batch_column(*impl_->native, row, column));
  });
}

struct WriteBatch::Impl {
  Impl() : native(ffi::native_write_batch_new()) {}
  rust::Box<ffi::NativeWriteBatch> native;
};

WriteBatch::WriteBatch() : impl_(std::make_unique<Impl>()) {}
WriteBatch::WriteBatch(WriteBatch&&) noexcept = default;
WriteBatch& WriteBatch::operator=(WriteBatch&&) noexcept = default;
WriteBatch::~WriteBatch() = default;

void WriteBatch::Put(BucketId bucket, BytesView key, ColumnIndex column,
                     BytesView value, const WriteOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  }
  const auto native_options = ToNative(options);
  ffi::native_write_batch_put(*impl_->native, bucket, RustBytes(key), column,
                              RustBytes(value), native_options);
}

void WriteBatch::Delete(BucketId bucket, BytesView key, ColumnIndex column,
                        const WriteOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  }
  const auto native_options = ToNative(options);
  ffi::native_write_batch_delete(*impl_->native, bucket, RustBytes(key), column,
                                 native_options);
}

void WriteBatch::Merge(BucketId bucket, BytesView key, ColumnIndex column,
                       BytesView value, const WriteOptions& options) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  }
  const auto native_options = ToNative(options);
  ffi::native_write_batch_merge(*impl_->native, bucket, RustBytes(key), column,
                                RustBytes(value), native_options);
}

std::size_t WriteBatch::size() const noexcept {
  if (!impl_) {
    return 0;
  }
  const auto count = ffi::native_write_batch_len(*impl_->native);
  return count > std::numeric_limits<std::size_t>::max()
             ? std::numeric_limits<std::size_t>::max()
             : static_cast<std::size_t>(count);
}

struct ScanCursor::Impl {
  explicit Impl(rust::Box<ffi::NativeScanCursor> native_cursor)
      : native(std::move(native_cursor)) {}
  rust::Box<ffi::NativeScanCursor> native;
};

ScanCursor::ScanCursor(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ScanCursor::ScanCursor(ScanCursor&&) noexcept = default;
ScanCursor& ScanCursor::operator=(ScanCursor&&) noexcept = default;
ScanCursor::~ScanCursor() = default;

OwnedBatch ScanCursor::Next(std::size_t max_rows) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ScanCursor has been moved from");
  }
  auto native = Translate([&] {
    return ffi::native_scan_cursor_next_owned(*impl_->native, max_rows);
  });
  return OwnedBatch(std::make_unique<OwnedBatch::Impl>(std::move(native)));
}

BufferResult ScanCursor::NextBatchInto(std::size_t max_rows,
                                       MutableBytesView output) {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ScanCursor has been moved from");
  }
  return ToBufferResult(Translate([&] {
    return ffi::native_scan_cursor_next_batch_into(
        *impl_->native, max_rows, RustBytes(output));
  }));
}

void ScanCursor::ResumeAfterBlockBoundary() {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "ScanCursor has been moved from");
  }
  ffi::native_scan_cursor_resume_after_block_boundary(*impl_->native);
}

struct Database::Impl {
  explicit Impl(rust::Box<ffi::NativeDatabase> native_database)
      : native(std::move(native_database)) {}
  rust::Box<ffi::NativeDatabase> native;
};

Database::Database(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
Database::Database(Database&&) noexcept = default;
Database& Database::operator=(Database&&) noexcept = default;
Database::~Database() = default;

Database Database::Open(std::string_view config_json) {
  auto native = Translate(
      [&] { return ffi::native_database_open(RustStr(config_json)); });
  return Database(std::make_unique<Impl>(std::move(native)));
}

Database Database::OpenFile(std::string_view config_path) {
  auto native = Translate(
      [&] { return ffi::native_database_open_file(RustStr(config_path)); });
  return Database(std::make_unique<Impl>(std::move(native)));
}

Database Database::Resume(std::string_view config_json, SnapshotId snapshot,
                          RecoveryMode mode) {
  auto native = Translate([&] {
    return ffi::native_database_resume(RustStr(config_json), snapshot,
                                       static_cast<std::uint8_t>(mode));
  });
  return Database(std::make_unique<Impl>(std::move(native)));
}

Database Database::ResumeFile(std::string_view config_path,
                              SnapshotId snapshot, RecoveryMode mode) {
  auto native = Translate([&] {
    return ffi::native_database_resume_file(RustStr(config_path), snapshot,
                                            static_cast<std::uint8_t>(mode));
  });
  return Database(std::make_unique<Impl>(std::move(native)));
}

void Database::Put(BucketId bucket, BytesView key, ColumnIndex column,
                   BytesView value, const WriteOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = ToNative(options);
  Translate([&] {
    ffi::native_database_put(*impl_->native, bucket, RustBytes(key), column,
                             RustBytes(value), native_options);
  });
}

void Database::Delete(BucketId bucket, BytesView key, ColumnIndex column,
                      const WriteOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = ToNative(options);
  Translate([&] {
    ffi::native_database_delete(*impl_->native, bucket, RustBytes(key), column,
                                native_options);
  });
}

void Database::Merge(BucketId bucket, BytesView key, ColumnIndex column,
                     BytesView value, const WriteOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = ToNative(options);
  Translate([&] {
    ffi::native_database_merge(*impl_->native, bucket, RustBytes(key), column,
                               RustBytes(value), native_options);
  });
}

void Database::Write(WriteBatch batch, bool await_durable) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  if (!batch.impl_) {
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  }
  Translate([&] {
    ffi::native_database_write_batch(*impl_->native,
                                     std::move(batch.impl_->native),
                                     await_durable);
  });
}

OwnedRow Database::Get(BucketId bucket, BytesView key,
                       const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = ToNative(options);
  auto native = Translate([&] {
    return ffi::native_database_get(*impl_->native, bucket, RustBytes(key),
                                    native_options);
  });
  return OwnedRow(std::make_unique<OwnedRow::Impl>(std::move(native)));
}

BufferResult Database::GetColumnInto(BucketId bucket, BytesView key,
                                     MutableBytesView output,
                                     const ReadOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = ToNative(options);
  return ToBufferResult(Translate([&] {
    return ffi::native_database_get_column_into(
        *impl_->native, bucket, RustBytes(key), RustBytes(output),
        native_options);
  }));
}

ScanCursor Database::Scan(BucketId bucket,
                          std::optional<BytesView> start_inclusive,
                          std::optional<BytesView> end_exclusive,
                          const ScanOptions& options) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  const auto native_options = ToNative(options);
  const BytesView start = start_inclusive.value_or(BytesView{});
  const BytesView end = end_exclusive.value_or(BytesView{});
  auto native = Translate([&] {
    return ffi::native_database_scan(
        *impl_->native, bucket, RustBytes(start), start_inclusive.has_value(),
        RustBytes(end), end_exclusive.has_value(), native_options);
  });
  return ScanCursor(std::make_unique<ScanCursor::Impl>(std::move(native)));
}

SnapshotId Database::Snapshot() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return Translate([&] { return ffi::native_database_snapshot(*impl_->native); });
}

bool Database::RetainSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return Translate([&] {
    return ffi::native_database_retain_snapshot(*impl_->native, snapshot);
  });
}

bool Database::ExpireSnapshot(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  return Translate([&] {
    return ffi::native_database_expire_snapshot(*impl_->native, snapshot);
  });
}

std::vector<SnapshotId> Database::ListSnapshots() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  auto native = Translate(
      [&] { return ffi::native_database_list_snapshots(*impl_->native); });
  return {native.begin(), native.end()};
}

std::string Database::SnapshotManifestJson(SnapshotId snapshot) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  auto native = Translate([&] {
    return ffi::native_database_snapshot_manifest_json(*impl_->native,
                                                       snapshot);
  });
  return {native.data(), native.size()};
}

void Database::SetTime(std::uint32_t unix_seconds) const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  ffi::native_database_set_time(*impl_->native, unix_seconds);
}

void Database::Close() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  Translate([&] { ffi::native_database_close(*impl_->native); });
}

std::string_view Version() noexcept {
  static const std::string version = [] {
    try {
      const auto native = ffi::native_database_version();
      return std::string(native.data(), native.size());
    } catch (...) {
      return std::string("unknown");
    }
  }();
  return version;
}

}  // namespace cobble
