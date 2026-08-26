#include "detail/convert.hpp"

#include <limits>

namespace cobble::structured::detail {

structured_ffi::NativeWriteOptions
ToNative(const cobble::WriteOptions &options) {
  structured_ffi::NativeWriteOptions native;
  native.has_ttl_seconds = options.ttl_seconds.has_value();
  native.ttl_seconds = options.ttl_seconds.value_or(0);
  native.has_column_family = options.column_family.has_value();
  native.column_family =
      rust::String(options.column_family.value_or(std::string{}));
  native.await_durable = options.await_durable;
  return native;
}

rust::Vec<structured_ffi::NativeBucketRange>
ToNativeRanges(std::span<const BucketRange> ranges) {
  rust::Vec<structured_ffi::NativeBucketRange> native;
  native.reserve(ranges.size());
  for (const auto &range : ranges) {
    structured_ffi::NativeBucketRange value;
    value.start_inclusive = range.start_inclusive;
    value.end_inclusive = range.end_inclusive;
    native.push_back(std::move(value));
  }
  return native;
}

rust::Vec<structured_ffi::NativeBytesDescriptor>
ToNativeElements(std::span<const BytesView> elements) {
  rust::Vec<structured_ffi::NativeBytesDescriptor> native;
  native.reserve(elements.size());
  for (const auto element : elements) {
    structured_ffi::NativeBytesDescriptor descriptor;
    descriptor.data = reinterpret_cast<std::size_t>(element.data());
    descriptor.length = element.size();
    native.push_back(std::move(descriptor));
  }
  return native;
}

structured_ffi::NativeListConfig ToNative(const ListConfig &config) {
  structured_ffi::NativeListConfig native;
  native.has_max_elements = config.max_elements.has_value();
  if (config.max_elements.has_value() &&
      *config.max_elements > std::numeric_limits<std::uint64_t>::max()) {
    throw Error(ErrorCode::kInput, "max_elements exceeds uint64_t");
  }
  native.max_elements =
      static_cast<std::uint64_t>(config.max_elements.value_or(0));
  native.retain_mode = static_cast<std::uint8_t>(config.retain_mode);
  native.preserve_element_ttl = config.preserve_element_ttl;
  return native;
}

Schema ToSchema(const structured_ffi::NativeStructuredSchema &native) {
  std::vector<ColumnFamilySchema> families;
  families.reserve(native.families.size());
  for (const auto &source_family : native.families) {
    ColumnFamilySchema family;
    family.name.assign(source_family.name.data(), source_family.name.size());
    family.id = source_family.id;
    family.explicit_columns.reserve(source_family.columns.size());
    for (const auto &source_column : source_family.columns) {
      ColumnType column;
      column.index = source_column.index;
      column.kind = static_cast<ColumnKind>(source_column.kind);
      if (source_column.list.has_max_elements) {
        if (source_column.list.max_elements >
            std::numeric_limits<std::size_t>::max()) {
          throw Error(ErrorCode::kFileFormat,
                      "schema max_elements exceeds size_t");
        }
        column.list.max_elements =
            static_cast<std::size_t>(source_column.list.max_elements);
      }
      column.list.retain_mode =
          static_cast<ListRetainMode>(source_column.list.retain_mode);
      column.list.preserve_element_ttl =
          source_column.list.preserve_element_ttl;
      family.explicit_columns.push_back(std::move(column));
    }
    families.push_back(std::move(family));
  }
  return Schema(std::move(families));
}

ShardSnapshot
ToShardSnapshot(const structured_ffi::NativeShardSnapshot &native) {
  ShardSnapshot result;
  result.db_id.assign(native.db_id.data(), native.db_id.size());
  result.snapshot_id = native.snapshot_id;
  result.manifest_path.assign(native.manifest_path.data(),
                              native.manifest_path.size());
  result.timestamp_seconds = native.timestamp_seconds;
  result.data_size_bytes = native.data_size_bytes;
  result.incremental_data_size_bytes = native.incremental_data_size_bytes;
  result.ranges.reserve(native.ranges.size());
  for (const auto &range : native.ranges) {
    result.ranges.push_back({range.start_inclusive, range.end_inclusive});
  }
  result.column_families.reserve(native.families.size());
  for (const auto &family : native.families) {
    result.column_families.push_back({std::string(family.name), family.id});
  }
  return result;
}

GlobalSnapshot ToGlobalSnapshot(const structured_ffi::NativeSnapshot &native) {
  GlobalSnapshot result{
      native.version,          native.id, native.total_buckets, {}, {},
      native.watermark_seconds};
  result.column_families.reserve(native.families.size());
  for (const auto &family : native.families) {
    result.column_families.push_back({std::string(family.name), family.id});
  }
  result.shards.reserve(native.shards.size());
  for (const auto &shard : native.shards) {
    result.shards.push_back(ToShardSnapshot(shard));
  }
  return result;
}

std::vector<MetricSample>
ToMetrics(rust::Vec<structured_ffi::NativeMetric> native) {
  std::vector<MetricSample> result;
  result.reserve(native.size());
  for (auto &metric : native) {
    MetricSample sample;
    sample.name = std::string(metric.name);
    sample.labels.reserve(metric.labels.size());
    for (const auto &label : metric.labels) {
      sample.labels.push_back(
          {std::string(label.key), std::string(label.value)});
    }
    switch (metric.kind) {
    case 0:
      sample.value = CounterValue{metric.counter};
      break;
    case 1:
      sample.value = GaugeValue{metric.gauge};
      break;
    case 2:
      sample.value =
          HistogramValue{metric.count, metric.sum, metric.min, metric.max};
      break;
    default:
      throw Error(ErrorCode::kFileFormat, "unknown metric value kind");
    }
    result.push_back(std::move(sample));
  }
  return result;
}

} // namespace cobble::structured::detail
