#pragma once

#include <cstddef>
#include <memory>

#include <cobble/options.hpp>
#include <cobble/types.hpp>

namespace cobble {

class COBBLE_CPP_API WriteBatch final {
 public:
  WriteBatch();
  WriteBatch(WriteBatch&&) noexcept;
  WriteBatch& operator=(WriteBatch&&) noexcept;
  ~WriteBatch();

  WriteBatch(const WriteBatch&) = delete;
  WriteBatch& operator=(const WriteBatch&) = delete;

  void Put(BucketId bucket, BytesView key, ColumnIndex column,
           BytesView value, const WriteOptions& options = {});
  void Delete(BucketId bucket, BytesView key, ColumnIndex column,
              const WriteOptions& options = {});
  void Merge(BucketId bucket, BytesView key, ColumnIndex column,
             BytesView value, const WriteOptions& options = {});
  [[nodiscard]] std::size_t size() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  friend class Database;
};

}  // namespace cobble
