cmake_minimum_required(VERSION 3.22)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/../../Cargo.toml" COBBLE_WORKSPACE_MANIFEST)
string(REGEX MATCH
       "\\[workspace\\.package\\][^[]*version[ \t]*=[ \t]*\"([^\"]+)\""
       COBBLE_VERSION_MATCH "${COBBLE_WORKSPACE_MANIFEST}")
if(NOT CMAKE_MATCH_1)
  message(FATAL_ERROR "Could not read the Cobble workspace version")
endif()

if(CMAKE_HOST_APPLE AND NOT DEFINED CMAKE_OSX_DEPLOYMENT_TARGET)
  set(CMAKE_OSX_DEPLOYMENT_TARGET "13.0" CACHE STRING
      "Minimum macOS version for both Rust and C++ objects")
endif()

project(cobble-cpp VERSION "${CMAKE_MATCH_1}" LANGUAGES CXX)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

option(COBBLE_CPP_BUILD_TESTS "Build the C++ binding tests" OFF)
option(COBBLE_CPP_BUILD_EXAMPLES "Build the C++ binding examples" OFF)
option(COBBLE_CPP_BUILD_S3_E2E_TEST
       "Build the C++ S3 snapshot/resume integration test" OFF)
set(COBBLE_CPP_CARGO_FEATURES "" CACHE STRING
    "Comma-separated Cargo features for cobble-cpp")

if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_program(COBBLE_CARGO_EXECUTABLE cargo REQUIRED)
find_package(Threads REQUIRED)

set(COBBLE_WORKSPACE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../..")
set(COBBLE_CPP_CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/cargo-target"
    CACHE PATH "Cargo target directory used by the C++ build")
set(COBBLE_CARGO_TARGET_DIR "${COBBLE_CPP_CARGO_TARGET_DIR}")
set(COBBLE_BRIDGE_INCLUDE_DIR
    "${COBBLE_CARGO_TARGET_DIR}/cxxbridge/cobble-cpp/src")

if(WIN32)
  set(COBBLE_RUST_LIBRARY_NAME "cobble_cpp_ffi.lib")
else()
  set(COBBLE_RUST_LIBRARY_NAME "libcobble_cpp_ffi.a")
endif()
set(COBBLE_RUST_LIBRARY_DEBUG
    "${COBBLE_CARGO_TARGET_DIR}/debug/${COBBLE_RUST_LIBRARY_NAME}")
set(COBBLE_RUST_LIBRARY_RELEASE
    "${COBBLE_CARGO_TARGET_DIR}/release/${COBBLE_RUST_LIBRARY_NAME}")
set(COBBLE_BRIDGE_HEADER "${COBBLE_BRIDGE_INCLUDE_DIR}/lib.rs.h")

set(COBBLE_CARGO_FEATURE_ARGS)
if(COBBLE_CPP_CARGO_FEATURES)
  list(APPEND COBBLE_CARGO_FEATURE_ARGS --features
       "${COBBLE_CPP_CARGO_FEATURES}")
endif()

set(COBBLE_CARGO_ENV "CARGO_TARGET_DIR=${COBBLE_CARGO_TARGET_DIR}")
if(APPLE)
  list(APPEND COBBLE_CARGO_ENV
       "MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}")
endif()

add_custom_target(
  cobble_cpp_cargo
  COMMAND
    "${CMAKE_COMMAND}" -E env ${COBBLE_CARGO_ENV}
    "${COBBLE_CARGO_EXECUTABLE}" build --manifest-path
    "${COBBLE_WORKSPACE_ROOT}/Cargo.toml" -p cobble-cpp
    $<IF:$<CONFIG:Debug>,--profile=dev,--release> ${COBBLE_CARGO_FEATURE_ARGS}
  BYPRODUCTS "${COBBLE_RUST_LIBRARY_DEBUG}" "${COBBLE_RUST_LIBRARY_RELEASE}"
             "${COBBLE_BRIDGE_HEADER}"
  WORKING_DIRECTORY "${COBBLE_WORKSPACE_ROOT}"
  USES_TERMINAL
  VERBATIM)

add_library(cobble_cpp_rust STATIC IMPORTED GLOBAL)
set_target_properties(
  cobble_cpp_rust
  PROPERTIES IMPORTED_CONFIGURATIONS "DEBUG;RELEASE;RELWITHDEBINFO;MINSIZEREL"
             IMPORTED_LOCATION_DEBUG "${COBBLE_RUST_LIBRARY_DEBUG}"
             IMPORTED_LOCATION_RELEASE "${COBBLE_RUST_LIBRARY_RELEASE}"
             IMPORTED_LOCATION_RELWITHDEBINFO "${COBBLE_RUST_LIBRARY_RELEASE}"
             IMPORTED_LOCATION_MINSIZEREL "${COBBLE_RUST_LIBRARY_RELEASE}")
add_dependencies(cobble_cpp_rust cobble_cpp_cargo)

add_library(cobble_cpp SHARED cpp/convert.cpp cpp/error.cpp cpp/options.cpp
                              cpp/write_batch.cpp cpp/scan.cpp cpp/database.cpp
                              cpp/multi_get.cpp cpp/snapshot.cpp cpp/schema.cpp
                              cpp/lifecycle.cpp cpp/metrics.cpp
                              cpp/sharded_db.cpp cpp/rescale.cpp cpp/version.cpp)
add_library(cobble::cobble ALIAS cobble_cpp)
add_dependencies(cobble_cpp cobble_cpp_cargo)

target_compile_features(cobble_cpp PUBLIC cxx_std_20)
target_compile_definitions(cobble_cpp PRIVATE COBBLE_CPP_BUILDING_LIBRARY)
target_include_directories(
  cobble_cpp
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE "${COBBLE_BRIDGE_INCLUDE_DIR}"
          "${COBBLE_CARGO_TARGET_DIR}/cxxbridge")
target_link_libraries(cobble_cpp PRIVATE cobble_cpp_rust Threads::Threads
                                         ${CMAKE_DL_LIBS})
set_target_properties(
  cobble_cpp
  PROPERTIES OUTPUT_NAME cobble_cpp
             EXPORT_NAME cobble
             VERSION "${PROJECT_VERSION}"
             SOVERSION "${PROJECT_VERSION_MAJOR}"
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN YES)

if(APPLE)
  find_library(COBBLE_CORE_FOUNDATION_FRAMEWORK CoreFoundation REQUIRED)
  find_library(COBBLE_SECURITY_FRAMEWORK Security REQUIRED)
  find_library(COBBLE_SYSTEM_CONFIGURATION_FRAMEWORK SystemConfiguration REQUIRED)
  target_link_libraries(
    cobble_cpp PRIVATE "${COBBLE_CORE_FOUNDATION_FRAMEWORK}"
                       "${COBBLE_SECURITY_FRAMEWORK}"
                       "${COBBLE_SYSTEM_CONFIGURATION_FRAMEWORK}")
elseif(WIN32)
  target_link_libraries(cobble_cpp PRIVATE advapi32 bcrypt ntdll ole32 shell32
                                           userenv ws2_32)
else()
  target_link_libraries(cobble_cpp PRIVATE m)
endif()

if(COBBLE_CPP_BUILD_EXAMPLES)
  add_executable(cobble_cpp_basic examples/basic.cpp)
  target_link_libraries(cobble_cpp_basic PRIVATE cobble::cobble)
endif()

if(COBBLE_CPP_BUILD_TESTS)
  enable_testing()
  add_executable(cobble_cpp_test tests/binding_test.cpp)
  target_link_libraries(cobble_cpp_test PRIVATE cobble::cobble)
  add_test(NAME cobble_cpp_binding COMMAND cobble_cpp_test)

  add_executable(cobble_cpp_bulk_e2e_test tests/bulk_e2e_test.cpp)
  target_link_libraries(cobble_cpp_bulk_e2e_test PRIVATE cobble::cobble)
  add_test(NAME cobble_cpp_bulk_e2e COMMAND cobble_cpp_bulk_e2e_test)
  set_tests_properties(cobble_cpp_bulk_e2e PROPERTIES TIMEOUT 180)

  add_executable(cobble_cpp_api_coverage_test tests/api_coverage_test.cpp)
  target_link_libraries(cobble_cpp_api_coverage_test PRIVATE cobble::cobble)
  add_test(NAME cobble_cpp_api_coverage COMMAND cobble_cpp_api_coverage_test)
  set_tests_properties(cobble_cpp_api_coverage PROPERTIES TIMEOUT 180)

  add_executable(cobble_cpp_raw_single_db_capability_test
                 tests/raw_single_db_capability_test.cpp)
  target_link_libraries(cobble_cpp_raw_single_db_capability_test
                        PRIVATE cobble::cobble)
  add_test(NAME cobble_cpp_raw_single_db_capability
           COMMAND cobble_cpp_raw_single_db_capability_test)
  set_tests_properties(cobble_cpp_raw_single_db_capability PROPERTIES TIMEOUT 180)

  add_executable(cobble_cpp_sharded_db_capability_test
                 tests/sharded_db_capability_test.cpp)
  target_link_libraries(cobble_cpp_sharded_db_capability_test
                        PRIVATE cobble::cobble)
  add_test(NAME cobble_cpp_sharded_db_capability
           COMMAND cobble_cpp_sharded_db_capability_test)
  set_tests_properties(cobble_cpp_sharded_db_capability PROPERTIES TIMEOUT 240)

  if(COBBLE_CPP_BUILD_S3_E2E_TEST)
    add_executable(cobble_cpp_s3_e2e_test tests/s3_e2e_test.cpp)
    target_link_libraries(cobble_cpp_s3_e2e_test PRIVATE cobble::cobble)
    add_test(NAME cobble_cpp_s3_e2e COMMAND cobble_cpp_s3_e2e_test)
    set_tests_properties(cobble_cpp_s3_e2e PROPERTIES TIMEOUT 180)
  endif()
endif()

if(COBBLE_CPP_BUILD_S3_E2E_TEST AND NOT COBBLE_CPP_BUILD_TESTS)
  message(FATAL_ERROR
          "COBBLE_CPP_BUILD_S3_E2E_TEST requires COBBLE_CPP_BUILD_TESTS=ON")
endif()

install(
  TARGETS cobble_cpp
  EXPORT cobbleCppTargets
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(
  FILES README.md ROW_BATCH_FORMAT.md DESIGN.md "${COBBLE_WORKSPACE_ROOT}/LICENSE"
  DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}/doc/cobble-cpp")
install(
  EXPORT cobbleCppTargets
  FILE cobble-cpp-targets.cmake
  NAMESPACE cobble::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/cobble-cpp")

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cobble-cpp-config.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cobble-cpp-config.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/cobble-cpp")
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/cobble-cpp-config-version.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/cobble-cpp-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/cobble-cpp-config-version.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/cobble-cpp")
