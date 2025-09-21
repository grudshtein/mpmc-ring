![ci](https://github.com/grudshtein/mpmc-ring/actions/workflows/ci.yml/badge.svg)

# MPMC Ring (bounded, lock-free) — C++20

Bounded multi-producer/multi-consumer ring buffer in C++20.  
Repo includes tests and a small benchmark with p50/p95/p99/p99.9 reporting.

## Build (Windows, Visual Studio 2022)
1. Open this folder in Visual Studio as a **CMake project**.
2. Set **x64 | Release** in the toolbar.
3. Choose startup item `tests` or `bench`, then run (Ctrl+F5).

**Run paths (typical VS CMake):**
- `out/build/x64-Release/tests.exe`
- `out/build/x64-Release/bench.exe`

## Command-line (any shell with CMake)
```bash
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release
build/Release/tests.exe
build/Release/bench.exe
