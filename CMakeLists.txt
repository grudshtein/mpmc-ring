cmake_minimum_required(VERSION 3.21)
project(mpmc_ring CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# MSVC AddressSanitizer toggle
option(ASAN "Enable AddressSanitizer on MSVC" ON)

# Library target (implementation file kept for now as a stub)
add_library(mpmc STATIC src/mpmc.cpp)
target_include_directories(mpmc PUBLIC ${CMAKE_SOURCE_DIR}/include)

# Executables
add_executable(tests tests/mpmc_tests.cpp)
target_link_libraries(tests PRIVATE mpmc)

add_executable(bench bench/runner.cpp)
target_link_libraries(bench PRIVATE mpmc)

# Compiler options
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
