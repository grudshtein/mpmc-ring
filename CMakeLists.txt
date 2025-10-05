cmake_minimum_required(VERSION 3.21)
project(mpmc_ring CXX)

# C++ mode
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Test infra + local CMake modules (e.g., Dependencies.cmake for gtest)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(CTest)         # defines BUILD_TESTING; integrates with ctest
enable_testing()
include(Dependencies)  # provides GTest::gtest / GTest::gtest_main via FetchContent

# Sanitizers: controllable from CMakePresets/CI
option(ENABLE_ASAN  "Enable AddressSanitizer" OFF)
option(ENABLE_TSAN  "Enable ThreadSanitizer (Clang/GCC only)" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer (Clang/GCC only)" OFF)

# Apply the chosen sanitizers to a target in a generator/toolchain-aware way
function(apply_sanitizers tgt)
  if (MSVC)
    if (ENABLE_ASAN)
      target_compile_options(${tgt} PRIVATE /fsanitize=address /Zi)
      target_link_options(${tgt} PRIVATE /DEBUG)
    endif()
  else()
    set(SAN_LIST "")
    if (ENABLE_ASAN)
      list(APPEND SAN_LIST address)
    endif()
    if (ENABLE_TSAN)
      list(APPEND SAN_LIST thread)
    endif()
    if (ENABLE_UBSAN)
      list(APPEND SAN_LIST undefined)
    endif()
    if (SAN_LIST)
      string(JOIN "," SAN_FLAGS ${SAN_LIST})
      target_compile_options(${tgt} PRIVATE -fsanitize=${SAN_FLAGS} -fno-omit-frame-pointer)
      target_link_options(${tgt}    PRIVATE -fsanitize=${SAN_FLAGS})
    endif()
  endif()
endfunction()

# Core library
add_library(mpmc STATIC src/mpmc.cpp)
target_include_directories(mpmc PUBLIC ${CMAKE_SOURCE_DIR}/include)

# Unit tests
add_executable(tests tests/mpmc_tests.cpp)
target_link_libraries(tests PRIVATE mpmc GTest::gtest GTest::gtest_main)
# Treat googletest includes as -isystem on GCC/Clang to keep warnings clean
get_target_property(GTEST_INC GTest::gtest INTERFACE_INCLUDE_DIRECTORIES)
if (GTEST_INC)
  target_include_directories(tests SYSTEM PRIVATE ${GTEST_INC})
endif()

# Benchmark binary
add_executable(bench bench/main.cpp bench/harness.cpp)
target_include_directories(bench PRIVATE ${CMAKE_SOURCE_DIR}/bench)
target_link_libraries(bench PRIVATE mpmc)

# Warnings: strict + warnings-as-errors, tuned per toolchain
if (MSVC)
  set(MSVC_WARN_OPTS
      /W4 /WX
      /permissive- /sdl
      /external:W0 /external:anglebrackets
      # alignment/padding warnings are noise for cacheline-sized structs:
      /wd4324
      # low-signal noise in optimized builds:
      /wd4514 /wd4710 /wd4711 /wd4820 /wd5045
  )
  foreach(tgt IN ITEMS mpmc tests bench)
    target_compile_options(${tgt} PRIVATE
      ${MSVC_WARN_OPTS}
      $<$<CONFIG:Release>:/O2 /DNDEBUG>
    )
  endforeach()
else()
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

# Turn on sanitizers for all build targets (no-op if toggles are OFF)
foreach(tgt IN ITEMS mpmc tests bench)
  apply_sanitizers(${tgt})
endforeach()

# Minimal ctest hook (the gtest runner lists tests internally)
if (BUILD_TESTING)
  include(GoogleTest)
  gtest_discover_tests(tests)  # lets VS/CTest enumerate all TEST() cases
endif()

# MSVC ASan: instrument googletest (avoids LNK2038 ASan-mismatch) and add /DEBUG for symbolized reports
if (MSVC AND ENABLE_ASAN)
  foreach(t IN ITEMS gtest gtest_main)
    if (TARGET ${t})
      target_compile_options(${t} PRIVATE /fsanitize=address /Zi)
      target_link_options(${t} PRIVATE /DEBUG)
    endif()
  endforeach()
endif()

# Copy MSVC ASan runtime DLL next to the binaries so ctest can run
if (MSVC AND ENABLE_ASAN)
  set(_asan_dll "")
  foreach(p
    "$ENV{VCToolsInstallDir}/bin/Hostx64/x64/clang_rt.asan_dynamic-x64.dll"
    "$ENV{VSINSTALLDIR}/VC/Tools/MSVC/${VCToolsVersion}/bin/Hostx64/x64/clang_rt.asan_dynamic-x64.dll"
    "$ENV{VSINSTALLDIR}/VC/Redist/MSVC/${VCToolsVersion}/asan/clang_rt.asan_dynamic-x64.dll"
  )
    if (EXISTS "${p}")
      set(_asan_dll "${p}")
      break()
    endif()
  endforeach()
  if (_asan_dll)
    foreach(tgt IN ITEMS tests bench)
      add_custom_command(TARGET ${tgt} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_asan_dll}" $<TARGET_FILE_DIR:${tgt}>)
    endforeach()
  else()
    message(WARNING "MSVC ASan runtime DLL not found; tests may fail with 0xc0000135.")
  endif()
endif()