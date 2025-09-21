#pragma once
#include <cstddef>

namespace mpmc {

// Placeholder API
template <typename T>
class MpmcRing {
public:
  explicit MpmcRing(std::size_t /*capacity_pow2*/) {}
  bool try_push(const T&) { return false; }
  bool try_push(T&&) { return false; }
  bool try_pop(T&) { return false; }
  std::size_t capacity() const noexcept { return 0; }
};

} // namespace mpmc
