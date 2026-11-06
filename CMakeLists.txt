# Standalone build for the print_wal extension + demo.
#
# The extension is a shared object loaded at runtime with LOAD EXTENSION, so it
# is compiled against the Ladybug headers but NOT linked against liblbug (the
# symbols resolve from the host process at dlopen time, like the in-tree
# extensions on Linux). The demo links against the built liblbug.so.
#
# Usage:
#   cmake -B /tmp/print_wal/build -S /tmp/print_wal \
#       -DLBUG_REPO=/home/ubuntu/src/ladybug
#   cmake --build /tmp/print_wal/build -j
#   /tmp/print_wal/build/demo
cmake_minimum_required(VERSION 3.20)
project(print_wal CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

if(NOT DEFINED LBUG_REPO)
    set(LBUG_REPO "/home/ubuntu/src/ladybug")
endif()

# --- Header search paths (mirror the in-tree extension builds) ----------------
set(LBUG_INCLUDE_DIRS
    ${LBUG_REPO}/src/include
    ${LBUG_REPO}/build/release/src/include
    ${LBUG_REPO}/third_party/antlr4_cypher/include
    ${LBUG_REPO}/third_party/antlr4_runtime/src
    ${LBUG_REPO}/third_party/brotli/c/include
    ${LBUG_REPO}/third_party/fast_float/include
    ${LBUG_REPO}/third_party/mbedtls/include
    ${LBUG_REPO}/third_party/parquet
    ${LBUG_REPO}/third_party/snappy
    ${LBUG_REPO}/third_party/thrift
    ${LBUG_REPO}/third_party/miniz
    ${LBUG_REPO}/third_party/yyjson/src
    ${LBUG_REPO}/third_party/pybind11/include
    ${LBUG_REPO}/third_party/pyparse
    ${LBUG_REPO}/third_party/re2/include
    ${LBUG_REPO}/third_party/alp/include
    ${LBUG_REPO}/third_party/spdlog
    ${LBUG_REPO}/third_party/utf8proc/include
    ${LBUG_REPO}/third_party/zstd/include
    ${LBUG_REPO}/third_party/httplib
    ${LBUG_REPO}/third_party/pcg
    ${LBUG_REPO}/third_party/lz4
    ${LBUG_REPO}/third_party/roaring_bitmap
    ${LBUG_REPO}/third_party/simsimd/include
)

# --- print_wal extension (LOAD EXTENSION target) ------------------------------
add_library(print_wal SHARED print_wal.cpp)
target_include_directories(print_wal PRIVATE ${LBUG_INCLUDE_DIRS})
target_compile_definitions(print_wal PRIVATE BUILD_DYNAMIC_LOAD)
set_target_properties(print_wal PROPERTIES
    OUTPUT_NAME "print_wal"
    PREFIX "lib"
    SUFFIX ".lbug_extension"
)

# --- demo driver ---------------------------------------------------------------
add_executable(demo demo.cpp)
target_include_directories(demo PRIVATE ${LBUG_INCLUDE_DIRS})
find_library(LBUG_LIB lbug PATHS ${LBUG_REPO}/build/release/src NO_DEFAULT_PATH)
if(NOT LBUG_LIB)
    message(FATAL_ERROR "liblbug not found under ${LBUG_REPO}/build/release/src; build ladybug first (make release).")
endif()
target_link_libraries(demo PRIVATE ${LBUG_LIB})
# Resolve liblbug.so at runtime without LD_LIBRARY_PATH.
set_target_properties(demo PROPERTIES
    BUILD_RPATH "${LBUG_REPO}/build/release/src"
    INSTALL_RPATH "${LBUG_REPO}/build/release/src"
)
