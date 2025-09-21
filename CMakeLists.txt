cmake_minimum_required(VERSION 3.21)
project(mpmc_ring CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# MSVC AddressSanitizer toggle (Windows-only; sanitizers mainly run in Linux CI)
option(ASAN "Enable AddressSanitizer on MSVC" OFF)
if (MSVC AND ASAN)
  message(STATUS "MSVC + ASAN requested; disabling (not supported reliably here).")
  set(ASAN OFF CACHE BOOL "Enable AddressSanitizer on MSVC" FORCE)
endif()

# ---- Testing & deps ----
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(CTest)          # defines BUILD_TESTING; provides ctest integration
enable_testing()
include(Dependencies)   # FetchContent googletest (defines GTest::gtest_main)

# ---- Library ----
add_library(mpmc STATIC src/mpmc.cpp)
target_include_directories(mpmc PUBLIC ${CMAKE_SOURCE_DIR}/include)

# ---- Executables ----
add_executable(tests tests/mpmc_tests.cpp)
target_link_libraries(tests PRIVATE mpmc GTest::gtest GTest::gtest_main)
get_target_property(GTEST_INC GTest::gtest INTERFACE_INCLUDE_DIRECTORIES)
if (GTEST_INC)
  target_include_directories(tests PRIVATE ${GTEST_INC})
endif()

add_executable(bench bench/runner.cpp)
target_link_libraries(bench PRIVATE mpmc)

# ---- Compiler options ----
if (MSVC)
  foreach(tgt IN ITEMS mpmc tests bench)
    target_compile_options(${tgt} PRIVATE /O2 /DNDEBUG /W4 /permissive-)
  endforeach()
  if (ASAN)
    add_link_options(/INCREMENTAL:NO)
    foreach(tgt IN ITEMS mpmc tests bench)
      target_compile_options(${tgt} PRIVATE /fsanitize=address)
      target_link_options(${tgt} PRIVATE /fsanitize=address)
    endforeach()
  endif()
else()
  foreach(tgt IN ITEMS mpmc tests bench)
    target_compile_options(${tgt} PRIVATE -O3 -DNDEBUG -Wall -Wextra -Wpedantic)
  endforeach()
endif()

# ---- ctest registration ----
if (BUILD_TESTING)
  add_test(NAME smoke COMMAND tests)
endif()
