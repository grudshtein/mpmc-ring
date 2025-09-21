#include <chrono>
#include <iostream>

int main() {
  const auto t0 = std::chrono::steady_clock::now();
  const auto t1 = std::chrono::steady_clock::now();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  std::cout << "[bench] placeholder ran in " << ms << " ms\n";
  return 0;
}
