#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace cobble {

struct MetricLabel {
  std::string key;
  std::string value;
};

struct CounterValue {
  std::uint64_t value;
};

struct GaugeValue {
  double value;
};

struct HistogramValue {
  std::uint64_t count;
  double sum;
  double min;
  double max;
};

using MetricValue =
    std::variant<CounterValue, GaugeValue, HistogramValue>;

struct MetricSample {
  std::string name;
  std::vector<MetricLabel> labels;
  MetricValue value;
};

}  // namespace cobble
