cmake_minimum_required(VERSION 3.20)
project(NimbusVault CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Option to build only client/workload and skip server-side dependencies (RocksDB)
option(NIMBUS_BUILD_CLIENT_ONLY "Build only client + workload (skip RocksDB and servers)" OFF)

if(APPLE)
  list(INSERT CMAKE_PREFIX_PATH 0 /opt/homebrew)
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

include(FetchContent)

FetchContent_Declare(spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG        v1.13.0)
FetchContent_MakeAvailable(spdlog)

FetchContent_Declare(googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        v1.14.0)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

if(NOT NIMBUS_BUILD_CLIENT_ONLY)
  # Try to find RocksDB for server-side components. If not present, we do a best-effort
  # attempt to locate system libraries but do not fatal-error here so local client-only
  # builds can proceed.
  find_package(RocksDB QUIET)
  if(NOT RocksDB_FOUND)
    if(APPLE)
      find_library(ROCKSDB_LIB rocksdb PATHS /opt/homebrew/lib NO_DEFAULT_PATH)
      find_path(ROCKSDB_INCLUDE rocksdb/db.h PATHS /opt/homebrew/include NO_DEFAULT_PATH)
      find_library(SNAPPY_LIB snappy PATHS /opt/homebrew/lib NO_DEFAULT_PATH)
    else()
      find_library(ROCKSDB_LIB rocksdb)
      find_path(ROCKSDB_INCLUDE rocksdb/db.h)
      find_library(SNAPPY_LIB snappy)
    endif()
    if(ROCKSDB_LIB)
      add_library(RocksDB::rocksdb UNKNOWN IMPORTED)
      set(_rocksdb_links "")
      if(SNAPPY_LIB)
        list(APPEND _rocksdb_links "${SNAPPY_LIB}")
      endif()
      set_target_properties(RocksDB::rocksdb PROPERTIES
        IMPORTED_LOCATION "${ROCKSDB_LIB}"
        INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INCLUDE}"
        INTERFACE_LINK_LIBRARIES "${_rocksdb_links}")
    endif()
  endif()
endif()

find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()
find_package(gRPC CONFIG REQUIRED)

set(PROTO_DIR "${CMAKE_SOURCE_DIR}/proto")
set(PROTO_OUT "${CMAKE_BINARY_DIR}/proto_gen")
file(MAKE_DIRECTORY "${PROTO_OUT}")

if(APPLE)
  find_program(PROTOC_BIN protoc PATHS /opt/homebrew/bin NO_DEFAULT_PATH)
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin PATHS /opt/homebrew/bin NO_DEFAULT_PATH)
endif()

if(NOT PROTOC_BIN)
  find_program(PROTOC_BIN protoc REQUIRED)
endif()
if(NOT GRPC_CPP_PLUGIN)
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

function(generate_grpc_proto PROTO_NAME)
  set(PROTO_FILE "${PROTO_DIR}/${PROTO_NAME}.proto")
  set(PB_H   "${PROTO_OUT}/${PROTO_NAME}.pb.h")
  set(PB_CC  "${PROTO_OUT}/${PROTO_NAME}.pb.cc")
  set(GRPC_H "${PROTO_OUT}/${PROTO_NAME}.grpc.pb.h")
  set(GRPC_CC "${PROTO_OUT}/${PROTO_NAME}.grpc.pb.cc")

  add_custom_command(
    OUTPUT "${PB_H}" "${PB_CC}" "${GRPC_H}" "${GRPC_CC}"
    COMMAND ${PROTOC_BIN}
      --proto_path=${PROTO_DIR}
      --cpp_out=${PROTO_OUT}
      --grpc_out=${PROTO_OUT}
      --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
      ${PROTO_FILE}
    DEPENDS "${PROTO_FILE}"
    COMMENT "Generating gRPC stubs for ${PROTO_NAME}.proto"
    VERBATIM
  )
  add_library(proto_${PROTO_NAME} STATIC "${PB_CC}" "${GRPC_CC}")
  target_include_directories(proto_${PROTO_NAME} PUBLIC "${PROTO_OUT}")
  target_link_libraries(proto_${PROTO_NAME}
    PUBLIC protobuf::libprotobuf gRPC::grpc++ gRPC::grpc++_reflection)
endfunction()

generate_grpc_proto(meta)
generate_grpc_proto(meta_repl)
generate_grpc_proto(storage)

add_library(nimbus_common INTERFACE)
target_include_directories(nimbus_common INTERFACE "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(nimbus_common INTERFACE spdlog::spdlog)

if(NOT NIMBUS_BUILD_CLIENT_ONLY)
add_library(nimbus_meta STATIC
  src/meta/meta_wal.cpp
  src/meta/meta_coordinator.cpp
  src/meta/meta_replication.cpp
  src/meta/meta_follower.cpp
  src/meta/meta_snapshot.cpp
  src/meta/meta_rpc.cpp
  src/meta/placement.cpp
  src/meta/policy.cpp
  src/meta/reconfig_driver.cpp
)
target_include_directories(nimbus_meta PUBLIC src)
target_link_libraries(nimbus_meta PUBLIC
  nimbus_common
  RocksDB::rocksdb
  proto_meta
  proto_meta_repl
  proto_storage
  gRPC::grpc++
)

add_library(nimbus_storage STATIC
  src/storage/chunk_store.cpp
  src/storage/replica_repair.cpp
  src/storage/storage_rpc.cpp
  src/storage/stats_reporter.cpp
)
target_include_directories(nimbus_storage PUBLIC src)
target_link_libraries(nimbus_storage PUBLIC
  nimbus_common
  RocksDB::rocksdb
  proto_storage
  proto_meta
  gRPC::grpc++
)

add_executable(meta_server src/bin/meta_server.cpp)
target_link_libraries(meta_server PRIVATE nimbus_meta)

add_executable(storage_server src/bin/storage_server.cpp)
target_link_libraries(storage_server PRIVATE nimbus_storage)
endif()

# Client + workload library
add_library(nimbus_client STATIC
  src/client/nimbus_client.cpp
  src/client/history_tracer.cpp
  src/workload/workload_gen.cpp
)
target_include_directories(nimbus_client PUBLIC src)
target_link_libraries(nimbus_client PUBLIC
  nimbus_common
  proto_meta
  proto_storage
  gRPC::grpc++
)

# Workload runner binary
add_executable(workload_runner src/bin/workload_runner.cpp)
target_link_libraries(workload_runner PRIVATE nimbus_client)

enable_testing()

if(NIMBUS_BUILD_CLIENT_ONLY)
  add_executable(client_unit_tests
    tests/unit/test_history_tracer.cpp
  )
  target_link_libraries(client_unit_tests PRIVATE
    nimbus_client
    GTest::gtest_main
  )
  target_include_directories(client_unit_tests PRIVATE src)
  include(GoogleTest)
  gtest_discover_tests(client_unit_tests)
else()
  add_executable(unit_tests
    tests/unit/test_meta_wal.cpp
    tests/unit/test_meta_coordinator.cpp
    tests/unit/test_meta_integration.cpp
    tests/unit/test_policy.cpp
    tests/unit/test_placement.cpp
    tests/unit/test_chunk_store.cpp
    tests/unit/test_history_tracer.cpp
    tests/unit/test_meta_integration.cpp
  )
  target_link_libraries(unit_tests PRIVATE
    nimbus_meta
    nimbus_storage
    nimbus_client
    GTest::gtest_main
  )
  target_include_directories(unit_tests PRIVATE src)
  include(GoogleTest)
  gtest_discover_tests(unit_tests)
endif()
