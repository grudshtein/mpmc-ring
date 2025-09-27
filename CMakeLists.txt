cmake_minimum_required(VERSION 3.21)
project(mpmc_ring CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
  # Silence third-party header warnings on GCC/Clang (-isystem).
  target_include_directories(tests SYSTEM PRIVATE ${GTEST_INC})
endif()

add_executable(bench bench/runner.cpp)
target_link_libraries(bench PRIVATE mpmc)

# ---- Compiler options (strict + warnings-as-errors) ----
if (MSVC)
  set(MSVC_WARN_OPTS
      /W4 /WX               # high warnings, treat as errors
      /permissive- /sdl     # standards mode + SDL checks
      /external:W0 /external:anglebrackets
      # (intentional/low-signal):
      /wd4324               # 'structure was padded due to alignment specifier' (alignas cacheline)
      /wd4514 /wd4710 /wd4711 /wd4820 /wd5045
  )
  foreach(tgt IN ITEMS mpmc tests bench)
    target_compile_options(${tgt} PRIVATE
      ${MSVC_WARN_OPTS}
      $<$<CONFIG:Release>:/O2 /DNDEBUG>
    )
  endforeach()
else()
  # GCC/Clang: strict warnings + treat warnings as errors.
  set(GCC_CLANG_WARN_OPTS
      -Wall -Wextra -Wpedantic -Werror
      -Wconversion -Wsign-conversion -Wshadow
      -Wformat=2 -Wnull-dereference
      -Woverloaded-virtual -Wnon-virtual-dtor
  )
  if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    list(APPEND GCC_CLANG_WARN_OPTS -Wextra-semi -Wdouble-promotion)
  elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    list(APPEND GCC_CLANG_WARN_OPTS -Wduplicated-cond -Wduplicated-branches -Wlogical-op -Wuseless-cast)
  endif()

  foreach(tgt IN ITEMS mpmc tests bench)
    target_compile_options(${tgt} PRIVATE
      ${GCC_CLANG_WARN_OPTS}
      $<$<CONFIG:Release>:-O3 -DNDEBUG>
    )
  endforeach()
endif()

# ---- ctest registration ----
if (BUILD_TESTING)
  add_test(NAME smoke COMMAND tests)
endif()
