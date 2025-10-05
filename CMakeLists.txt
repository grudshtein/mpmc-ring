cmake_minimum_required(VERSION 3.21)
project(mpmc_ring CXX)

# C++ mode
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Test infra + local CMake modules (Dependencies.cmake provides gtest)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(CTest)
enable_testing()
include(Dependencies)

# Sanitizers (toggled via presets/CI)
option(ENABLE_ASAN  "Enable AddressSanitizer" OFF)
option(ENABLE_TSAN  "Enable ThreadSanitizer (Clang/GCC only)" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer (Clang/GCC only)" OFF)

# Apply selected sanitizers to a target
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

# MSVC ASan: instrument googletest too (avoids ASan mismatch); keep symbols for reports
if (MSVC AND ENABLE_ASAN)
  foreach(t IN ITEMS gtest gtest_main)
    if (TARGET ${t})
      target_compile_options(${t} PRIVATE /fsanitize=address /Zi)
      target_link_options(${t} PRIVATE /DEBUG)
    endif()
  endforeach()
endif()

# Core library
add_library(mpmc STATIC src/mpmc.cpp)
target_include_directories(mpmc PUBLIC ${CMAKE_SOURCE_DIR}/include)

# Unit tests
add_executable(tests tests/mpmc_tests.cpp)
target_link_libraries(tests PRIVATE mpmc GTest::gtest GTest::gtest_main)
# Treat gtest headers as system on GCC/Clang to keep warnings clean
get_target_property(GTEST_INC GTest::gtest INTERFACE_INCLUDE_DIRECTORIES)
if (GTEST_INC)
  target_include_directories(tests SYSTEM PRIVATE ${GTEST_INC})
endif()

# Benchmark binary
add_executable(bench bench/main.cpp bench/harness.cpp)
target_include_directories(bench PRIVATE ${CMAKE_SOURCE_DIR}/bench)
target_link_libraries(bench PRIVATE mpmc)

# Warnings: strict + Werror, tuned per toolchain
if (MSVC)
  set(MSVC_WARN_OPTS
      /W4 /WX
      /permissive- /sdl
      /external:W0 /external:anglebrackets
      /wd4324
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

# Turn on sanitizers for all build targets
foreach(tgt IN ITEMS mpmc tests bench)
  apply_sanitizers(${tgt})
endforeach()

# MSVC ASan: copy runtime DLL beside tests/bench so ctest can run on CI runners
if (MSVC AND ENABLE_ASAN)
  # Prefer toolchain + VS root envs that don't contain parentheses
  set(_asan_dll "")
  set(_candidates "")

  if(DEFINED ENV{VCToolsInstallDir})
    list(APPEND _candidates
      "$ENV{VCToolsInstallDir}/bin/Hostx64/x64/clang_rt.asan*_dynamic-x64.dll")
  endif()
  if(DEFINED ENV{VSINSTALLDIR})
    list(APPEND _candidates
      "$ENV{VSINSTALLDIR}/VC/Redist/MSVC/*/asan/clang_rt.asan*_dynamic-x64.dll"
      "$ENV{VSINSTALLDIR}/VC/Redist/MSVC/*/AddressSanitizer/x64/clang_rt.asan*_dynamic-x64.dll")
  endif()

  foreach(p IN LISTS _candidates)
    file(GLOB _hits "${p}")
    if(_hits)
      list(GET _hits 0 _asan_dll)
      break()
    endif()
  endforeach()

  if(_asan_dll)
    message(STATUS "MSVC ASan runtime: ${_asan_dll}")
    foreach(tgt IN ITEMS tests bench)
      add_custom_command(TARGET ${tgt} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_asan_dll}" $<TARGET_FILE_DIR:${tgt}>)
    endforeach()
  else()
    message(WARNING "ASan DLL not found (checked VCToolsInstallDir/VSINSTALLDIR); tests may fail with 0xc0000135.")
  endif()
endif()

# Build a PATH prefix that points to MSVC ASan redist dirs on runners
function(get_msvc_asan_path outvar)
  set(_dirs "")
  if(DEFINED ENV{VCToolsRedistDir})
    list(APPEND _dirs
      "$ENV{VCToolsRedistDir}/asan"
      "$ENV{VCToolsRedistDir}/AddressSanitizer/x64")
  endif()
  if(DEFINED ENV{VSINSTALLDIR} AND DEFINED ENV{VCToolsVersion})
    list(APPEND _dirs
      "$ENV{VSINSTALLDIR}/VC/Redist/MSVC/$ENV{VCToolsVersion}/asan"
      "$ENV{VSINSTALLDIR}/VC/Redist/MSVC/$ENV{VCToolsVersion}/AddressSanitizer/x64")
  endif()
  string(JOIN ";" _joined ${_dirs})
  set(${outvar} "${_joined}" PARENT_SCOPE)
endfunction()

# ctest: discover individual TEST() cases (better for IDEs/Test Explorer)
if (BUILD_TESTING)
  include(GoogleTest)
  if (MSVC AND ENABLE_ASAN)
    get_msvc_asan_path(_asan_path_prefix)
    if (_asan_path_prefix)
      gtest_discover_tests(tests
        PROPERTIES ENVIRONMENT "PATH=${_asan_path_prefix};$ENV{PATH}")
    else()
      gtest_discover_tests(tests)
      message(WARNING "ASan redist dirs not found; tests may fail with 0xc0000135.")
    endif()
  else()
    gtest_discover_tests(tests)
  endif()
endif()