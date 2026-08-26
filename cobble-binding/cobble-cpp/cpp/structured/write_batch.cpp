#include <cobble/structured/db.hpp>
#include <cobble/structured/single_db.hpp>
#include <cobble/structured/write_batch.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "../detail/error.hpp"
#include "detail/convert.hpp"
#include "detail/impl.hpp"

namespace cobble::structured {
namespace {

constexpr std::uint16_t kHasTtl = 1;
constexpr std::uint16_t kHasFamily = 2;
constexpr std::uint16_t kAwaitDurable = 4;

constexpr std::size_t AlignUp(std::size_t value, std::size_t alignment) {
  return (value + alignment - 1) / alignment * alignment;
}

struct BytesDescriptor {
  std::size_t data;
  std::size_t length;
};

struct WriteOperationDescriptor {
  std::uint8_t operation;
  std::uint8_t kind;
  std::uint16_t bucket;
  std::uint16_t column;
  std::uint16_t flags;
  std::uint32_t ttl_seconds;
  std::uint32_t reserved;
  std::size_t key_data;
  std::size_t key_length;
  std::size_t value_data;
  std::size_t value_length;
  std::size_t elements_data;
  std::size_t element_count;
  std::size_t family_data;
  std::size_t family_length;
};

static_assert(std::is_standard_layout_v<BytesDescriptor>);
static_assert(offsetof(BytesDescriptor, data) == 0);
static_assert(offsetof(BytesDescriptor, length) == sizeof(std::size_t));
static_assert(sizeof(BytesDescriptor) == sizeof(std::size_t) * 2);
static_assert(alignof(BytesDescriptor) == alignof(std::size_t));

constexpr std::size_t kPayloadOffset =
    AlignUp(sizeof(std::uint8_t) * 2 + sizeof(std::uint16_t) * 3 +
                sizeof(std::uint32_t) * 2,
            alignof(std::size_t));
constexpr std::size_t kOperationSize =
    AlignUp(kPayloadOffset + sizeof(std::size_t) * 8, alignof(std::size_t));
static_assert(std::is_standard_layout_v<WriteOperationDescriptor>);
static_assert(offsetof(WriteOperationDescriptor, operation) == 0);
static_assert(offsetof(WriteOperationDescriptor, kind) == 1);
static_assert(offsetof(WriteOperationDescriptor, bucket) == 2);
static_assert(offsetof(WriteOperationDescriptor, column) == 4);
static_assert(offsetof(WriteOperationDescriptor, flags) == 6);
static_assert(offsetof(WriteOperationDescriptor, ttl_seconds) == 8);
static_assert(offsetof(WriteOperationDescriptor, reserved) == 12);
static_assert(offsetof(WriteOperationDescriptor, key_data) == kPayloadOffset);
static_assert(offsetof(WriteOperationDescriptor, key_length) ==
              kPayloadOffset + sizeof(std::size_t));
static_assert(offsetof(WriteOperationDescriptor, value_data) ==
              kPayloadOffset + sizeof(std::size_t) * 2);
static_assert(offsetof(WriteOperationDescriptor, value_length) ==
              kPayloadOffset + sizeof(std::size_t) * 3);
static_assert(offsetof(WriteOperationDescriptor, elements_data) ==
              kPayloadOffset + sizeof(std::size_t) * 4);
static_assert(offsetof(WriteOperationDescriptor, element_count) ==
              kPayloadOffset + sizeof(std::size_t) * 5);
static_assert(offsetof(WriteOperationDescriptor, family_data) ==
              kPayloadOffset + sizeof(std::size_t) * 6);
static_assert(offsetof(WriteOperationDescriptor, family_length) ==
              kPayloadOffset + sizeof(std::size_t) * 7);
static_assert(sizeof(WriteOperationDescriptor) == kOperationSize);
static_assert(alignof(WriteOperationDescriptor) == alignof(std::size_t));

std::size_t Address(BytesView bytes) noexcept {
  return bytes.empty() ? 0 : reinterpret_cast<std::size_t>(bytes.data());
}

std::uint64_t Count(std::size_t value) {
  if (value > std::numeric_limits<std::uint64_t>::max()) {
    throw Error(ErrorCode::kInput, "write operation count exceeds uint64_t");
  }
  return static_cast<std::uint64_t>(value);
}

struct DescriptorStorage {
  std::vector<BytesDescriptor> elements;
  std::vector<WriteOperationDescriptor> operations;
};

DescriptorStorage Describe(std::span<const WriteOperation> input) {
  DescriptorStorage result;
  std::size_t element_count = 0;
  for (const auto &operation : input) {
    if (operation.kind == ColumnKind::kList) {
      if (operation.list_elements.size() >
          std::numeric_limits<std::size_t>::max() - element_count) {
        throw Error(ErrorCode::kInput, "LIST element count overflows size_t");
      }
      element_count += operation.list_elements.size();
    }
  }
  result.elements.reserve(element_count);
  result.operations.reserve(input.size());
  for (const auto &operation : input) {
    const auto operation_tag = static_cast<std::uint8_t>(operation.operation);
    const auto kind_tag = static_cast<std::uint8_t>(operation.kind);
    if (operation_tag > static_cast<std::uint8_t>(WriteOperationType::kDelete))
      throw Error(ErrorCode::kInput, "unknown structured write operation");
    if (kind_tag > static_cast<std::uint8_t>(ColumnKind::kList))
      throw Error(ErrorCode::kInput, "unknown structured column kind");
    if (operation.kind == ColumnKind::kBytes &&
        !operation.list_elements.empty())
      throw Error(ErrorCode::kInput,
                  "BYTES operation cannot contain LIST elements");
    if (operation.kind == ColumnKind::kList && !operation.bytes_value.empty())
      throw Error(ErrorCode::kInput,
                  "LIST operation cannot contain a BYTES value");
    if (operation.operation == WriteOperationType::kDelete &&
        (!operation.bytes_value.empty() || !operation.list_elements.empty()))
      throw Error(ErrorCode::kInput, "delete operation cannot contain a value");

    const auto element_start = result.elements.size();
    for (const auto element : operation.list_elements) {
      result.elements.push_back({Address(element), element.size()});
    }
    const auto &options = operation.options;
    std::uint16_t flags = options.await_durable ? kAwaitDurable : 0;
    if (options.ttl_seconds)
      flags |= kHasTtl;
    if (options.column_family) {
      if (options.column_family->empty())
        throw Error(ErrorCode::kInput, "column family must not be empty");
      flags |= kHasFamily;
    }
    const auto family = options.column_family
                            ? BytesView(reinterpret_cast<const Byte *>(
                                            options.column_family->data()),
                                        options.column_family->size())
                            : BytesView{};
    const auto *elements = operation.list_elements.empty()
                               ? nullptr
                               : result.elements.data() + element_start;
    result.operations.push_back(
        {operation_tag, kind_tag, operation.bucket, operation.column, flags,
         options.ttl_seconds.value_or(0), 0, Address(operation.key),
         operation.key.size(), Address(operation.bytes_value),
         operation.bytes_value.size(), reinterpret_cast<std::size_t>(elements),
         operation.list_elements.size(), Address(family), family.size()});
  }
  return result;
}

std::size_t DescriptorAddress(const DescriptorStorage &storage) noexcept {
  return storage.operations.empty()
             ? 0
             : reinterpret_cast<std::size_t>(storage.operations.data());
}

} // namespace

struct WriteBatch::Impl {
  struct Element {
    std::size_t offset;
    std::size_t length;
  };
  struct Operation {
    WriteOperationType operation;
    ColumnKind kind;
    BucketId bucket;
    ColumnIndex column;
    std::size_t key_offset;
    std::size_t key_length;
    std::size_t value_offset;
    std::size_t value_length;
    std::size_t element_start;
    std::size_t element_count;
    cobble::WriteOptions options;
  };

  std::vector<Byte> arena;
  std::vector<Element> elements;
  std::vector<Operation> operations;
  mutable std::vector<BytesView> element_views;
  mutable std::vector<WriteOperation> borrowed_operations;

  std::pair<std::size_t, std::size_t> Copy(BytesView value) {
    if (value.size() > std::numeric_limits<std::size_t>::max() - arena.size())
      throw Error(ErrorCode::kInput,
                  "WriteBatch byte storage overflows size_t");
    const auto offset = arena.size();
    if (value.empty())
      return {offset, 0};
    arena.insert(arena.end(), value.begin(), value.end());
    return {offset, value.size()};
  }

  BytesView View(std::size_t offset, std::size_t length) const noexcept {
    return length == 0 ? BytesView{} : BytesView(arena.data() + offset, length);
  }

  void Append(WriteOperationType operation, ColumnKind kind, BucketId bucket,
              BytesView key, ColumnIndex column, BytesView value,
              std::span<const BytesView> list,
              const cobble::WriteOptions &options) {
    const auto [key_offset, key_length] = Copy(key);
    const auto [value_offset, value_length] = Copy(value);
    const auto element_start = elements.size();
    for (const auto item : list) {
      const auto [offset, length] = Copy(item);
      elements.push_back({offset, length});
    }
    operations.push_back({operation, kind, bucket, column, key_offset,
                          key_length, value_offset, value_length, element_start,
                          list.size(), options});
  }

  std::span<const WriteOperation> Materialize() const {
    element_views.resize(elements.size());
    for (std::size_t index = 0; index < elements.size(); ++index) {
      element_views[index] =
          View(elements[index].offset, elements[index].length);
    }
    borrowed_operations.resize(operations.size());
    for (std::size_t index = 0; index < operations.size(); ++index) {
      const auto &source = operations[index];
      const auto list = source.element_count == 0
                            ? std::span<const BytesView>{}
                            : std::span<const BytesView>(
                                  element_views.data() + source.element_start,
                                  source.element_count);
      borrowed_operations[index] = {
          source.operation,
          source.kind,
          source.bucket,
          source.column,
          View(source.key_offset, source.key_length),
          View(source.value_offset, source.value_length),
          list,
          source.options};
    }
    return borrowed_operations;
  }

  void Clear() noexcept {
    arena.clear();
    elements.clear();
    operations.clear();
    element_views.clear();
    borrowed_operations.clear();
  }
};

WriteOperation WriteOperation::PutBytes(BucketId bucket, BytesView key,
                                        ColumnIndex column, BytesView value,
                                        cobble::WriteOptions options) {
  return {WriteOperationType::kPut,
          ColumnKind::kBytes,
          bucket,
          column,
          key,
          value,
          {},
          std::move(options)};
}
WriteOperation WriteOperation::PutList(BucketId bucket, BytesView key,
                                       ColumnIndex column,
                                       std::span<const BytesView> elements,
                                       cobble::WriteOptions options) {
  return {WriteOperationType::kPut,
          ColumnKind::kList,
          bucket,
          column,
          key,
          {},
          elements,
          std::move(options)};
}
WriteOperation WriteOperation::MergeBytes(BucketId bucket, BytesView key,
                                          ColumnIndex column, BytesView value,
                                          cobble::WriteOptions options) {
  return {WriteOperationType::kMerge,
          ColumnKind::kBytes,
          bucket,
          column,
          key,
          value,
          {},
          std::move(options)};
}
WriteOperation WriteOperation::MergeList(BucketId bucket, BytesView key,
                                         ColumnIndex column,
                                         std::span<const BytesView> elements,
                                         cobble::WriteOptions options) {
  return {WriteOperationType::kMerge,
          ColumnKind::kList,
          bucket,
          column,
          key,
          {},
          elements,
          std::move(options)};
}
WriteOperation WriteOperation::Delete(BucketId bucket, BytesView key,
                                      ColumnIndex column,
                                      cobble::WriteOptions options) {
  return {WriteOperationType::kDelete,
          ColumnKind::kBytes,
          bucket,
          column,
          key,
          {},
          {},
          std::move(options)};
}

WriteBatch::WriteBatch() : impl_(std::make_unique<Impl>()) {}
WriteBatch::WriteBatch(WriteBatch &&) noexcept = default;
WriteBatch &WriteBatch::operator=(WriteBatch &&) noexcept = default;
WriteBatch::~WriteBatch() = default;

void WriteBatch::PutBytes(BucketId bucket, BytesView key, ColumnIndex column,
                          BytesView value,
                          const cobble::WriteOptions &options) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  impl_->Append(WriteOperationType::kPut, ColumnKind::kBytes, bucket, key,
                column, value, {}, options);
}
void WriteBatch::PutList(BucketId bucket, BytesView key, ColumnIndex column,
                         std::span<const BytesView> elements,
                         const cobble::WriteOptions &options) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  impl_->Append(WriteOperationType::kPut, ColumnKind::kList, bucket, key,
                column, {}, elements, options);
}
void WriteBatch::MergeBytes(BucketId bucket, BytesView key, ColumnIndex column,
                            BytesView value,
                            const cobble::WriteOptions &options) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  impl_->Append(WriteOperationType::kMerge, ColumnKind::kBytes, bucket, key,
                column, value, {}, options);
}
void WriteBatch::MergeList(BucketId bucket, BytesView key, ColumnIndex column,
                           std::span<const BytesView> elements,
                           const cobble::WriteOptions &options) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  impl_->Append(WriteOperationType::kMerge, ColumnKind::kList, bucket, key,
                column, {}, elements, options);
}
void WriteBatch::Delete(BucketId bucket, BytesView key, ColumnIndex column,
                        const cobble::WriteOptions &options) {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  impl_->Append(WriteOperationType::kDelete, ColumnKind::kBytes, bucket, key,
                column, {}, {}, options);
}
void WriteBatch::Clear() noexcept {
  if (impl_)
    impl_->Clear();
}
std::size_t WriteBatch::size() const noexcept {
  return impl_ ? impl_->operations.size() : 0;
}
bool WriteBatch::empty() const noexcept { return size() == 0; }

void Db::Write(std::span<const WriteOperation> operations) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState, "structured Db has been moved from");
  const auto descriptors = Describe(operations);
  detail::Translate([&] {
    structured_ffi::native_structured_db_write(*impl_->native,
                                               DescriptorAddress(descriptors),
                                               Count(operations.size()));
  });
}
void Db::Write(WriteBatch &batch) const {
  if (!batch.impl_)
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  Write(batch.impl_->Materialize());
  batch.impl_->Clear();
}
void SingleDb::Write(std::span<const WriteOperation> operations) const {
  if (!impl_)
    throw Error(ErrorCode::kInvalidState,
                "structured SingleDb has been moved from");
  const auto descriptors = Describe(operations);
  detail::Translate([&] {
    structured_ffi::native_structured_single_db_write(
        *impl_->native, DescriptorAddress(descriptors),
        Count(operations.size()));
  });
}
void SingleDb::Write(WriteBatch &batch) const {
  if (!batch.impl_)
    throw Error(ErrorCode::kInvalidState, "WriteBatch has been moved from");
  Write(batch.impl_->Materialize());
  batch.impl_->Clear();
}

} // namespace cobble::structured
