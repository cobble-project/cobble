#include <cobble/cobble.hpp>

#include <cstdint>
#include <iostream>
#include <string>

namespace {

cobble::BytesView Bytes(std::string_view value) {
  return {reinterpret_cast<const std::uint8_t*>(value.data()), value.size()};
}

std::string String(cobble::BytesView value) {
  return {reinterpret_cast<const char*>(value.data()), value.size()};
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: cobble-cpp-basic <database-directory>\n";
    return 2;
  }

  const std::string config =
      R"({"volumes":[{"base_dir":"file://)" + std::string(argv[1]) +
      R"(","kinds":["meta","primary_data_priority_high"]}],"num_columns":1})";

  try {
    auto db = cobble::Database::Open(config);
    db.Put(0, Bytes("hello"), 0, Bytes("world"));

    auto row = db.Get(0, Bytes("hello"));
    if (row.found() && row.has_column(0)) {
      std::cout << String(row.column(0)) << '\n';
    }

    const auto snapshot = db.Snapshot();
    std::cout << "queued snapshot " << snapshot << '\n';
  } catch (const cobble::Error& error) {
    std::cerr << "Cobble error (" << static_cast<int>(error.code())
              << "): " << error.what() << '\n';
    return 1;
  }
}
