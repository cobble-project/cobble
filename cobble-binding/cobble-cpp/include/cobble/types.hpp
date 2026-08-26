#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>

#if defined(_WIN32) && !defined(COBBLE_CPP_STATIC)
#if defined(COBBLE_CPP_BUILDING_LIBRARY)
#define COBBLE_CPP_API __declspec(dllexport)
#else
#define COBBLE_CPP_API __declspec(dllimport)
#endif
#elif defined(__GNUC__) && !defined(COBBLE_CPP_STATIC)
#define COBBLE_CPP_API __attribute__((visibility("default")))
#else
#define COBBLE_CPP_API
#endif

namespace cobble {

using Byte = std::uint8_t;
using BytesView = std::span<const Byte>;
using MutableBytesView = std::span<Byte>;
using BucketId = std::uint16_t;
using ColumnIndex = std::uint16_t;
using SnapshotId = std::uint64_t;

enum class ErrorCode : std::uint8_t {
  kUnknown = 0,
  kUrl = 1,
  kFileSystem = 2,
  kIo = 3,
  kMemtableFull = 4,
  kConfiguration = 5,
  kInput = 6,
  kCoordination = 7,
  kInvalidState = 8,
  kFileFormat = 9,
  kChecksum = 10,
  kCancelled = 11,
};

class COBBLE_CPP_API Error final : public std::runtime_error {
 public:
  Error(ErrorCode code, std::string message);

  [[nodiscard]] ErrorCode code() const noexcept;

 private:
  ErrorCode code_;
};

enum class RecoveryMode : std::uint8_t {
  kSnapshotOnly = 0,
  kLatestWithWal = 1,
};

enum class BufferStatus : std::uint8_t {
  kOk = 0,
  kNotFound = 1,
  kEnd = 2,
  kBufferTooSmall = 3,
  kBlockBoundary = 4,
};

struct BufferResult {
  BufferStatus status = BufferStatus::kOk;
  std::size_t bytes_written = 0;
  std::size_t bytes_required = 0;
  std::size_t row_count = 0;
};

}  // namespace cobble
