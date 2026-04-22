cmake_minimum_required(VERSION 3.20)
project(NimbusVault CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(INSERT CMAKE_PREFIX_PATH 0 /opt/homebrew)

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

find_package(RocksDB QUIET)
if(NOT RocksDB_FOUND)
  find_library(ROCKSDB_LIB rocksdb PATHS /opt/homebrew/lib NO_DEFAULT_PATH REQUIRED)
  find_path(ROCKSDB_INCLUDE rocksdb/db.h PATHS /opt/homebrew/include NO_DEFAULT_PATH REQUIRED)
  find_library(SNAPPY_LIB snappy PATHS /opt/homebrew/lib NO_DEFAULT_PATH REQUIRED)
  add_library(RocksDB::rocksdb UNKNOWN IMPORTED)
  set_target_properties(RocksDB::rocksdb PROPERTIES
    IMPORTED_LOCATION "${ROCKSDB_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INCLUDE}"
    INTERFACE_LINK_LIBRARIES "${SNAPPY_LIB}")
endif()

find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

set(PROTO_DIR "${CMAKE_SOURCE_DIR}/proto")
set(PROTO_OUT "${CMAKE_BINARY_DIR}/proto_gen")
file(MAKE_DIRECTORY "${PROTO_OUT}")

find_program(PROTOC_BIN protoc PATHS /opt/homebrew/bin NO_DEFAULT_PATH REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin PATHS /opt/homebrew/bin NO_DEFAULT_PATH REQUIRED)

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

add_executable(meta_server src/bin/meta_server.cpp)
target_link_libraries(meta_server PRIVATE nimbus_meta)

enable_testing()

add_executable(unit_tests
  tests/unit/test_meta_wal.cpp
  tests/unit/test_meta_coordinator.cpp
  tests/unit/test_policy.cpp
  tests/unit/test_placement.cpp
)
target_link_libraries(unit_tests PRIVATE
  nimbus_meta
  GTest::gtest_main
)
target_include_directories(unit_tests PRIVATE src)

include(GoogleTest)
gtest_discover_tests(unit_tests)
