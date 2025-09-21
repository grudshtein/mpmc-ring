include(FetchContent)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)  # MSVC: use /MD runtime for gtest

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
FetchContent_MakeAvailable(googletest)
