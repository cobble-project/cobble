#include <cobble/database.hpp>

#include <utility>

#include "detail/impl.hpp"

namespace cobble {

std::vector<MetricSample> Database::Metrics() const {
  if (!impl_) {
    throw Error(ErrorCode::kInvalidState, "Database has been moved from");
  }
  auto native = ffi::native_database_metrics(*impl_->native);
  std::vector<MetricSample> result;
  result.reserve(native.size());
  for (const auto& sample : native) {
    std::vector<MetricLabel> labels;
    labels.reserve(sample.labels.size());
    for (const auto& label : sample.labels) {
      labels.push_back(
          {std::string(label.key), std::string(label.value)});
    }

    MetricValue value;
    switch (sample.kind) {
      case 0:
        value = CounterValue{sample.counter};
        break;
      case 1:
        value = GaugeValue{sample.gauge};
        break;
      case 2:
        value = HistogramValue{sample.count, sample.sum, sample.min,
                               sample.max};
        break;
      default:
        throw Error(ErrorCode::kInvalidState,
                    "native metric has an unknown value kind");
    }
    result.push_back(
        {std::string(sample.name), std::move(labels), std::move(value)});
  }
  return result;
}

}  // namespace cobble
