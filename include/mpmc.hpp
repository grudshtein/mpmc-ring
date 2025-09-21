#pragma once
#include <cstddef>
#include <stdexcept>

namespace mpmc {

// Placeholder API
template <typename T>
class MpmcRing {
public:
  /// Construct a fixed-capacity ring.
  /// @param capacity Must be power-of-two and >= 2; fixed for object lifetime.
  /// @throws std::invalid_argument if precondition is violated.
  explicit MpmcRing(std::size_t capacity) : capacity_{capacity}, mask_{capacity_ - 1} {
    if (capacity < 2) {
      throw std::invalid_argument("capacity must be >= 2");
    }
    if ((capacity & (capacity - 1)) != 0) {
      throw std::invalid_argument("capacity must be a power of 2");
    }
  }

  /// Push by copy.
  /// @return true on success; false if the ring is full.
  bool try_push(const T& v) noexcept { return false; }

  /// Push by move.
  /// @return true on success; false if the ring is full.
  bool try_push(T&& v) noexcept { return false; }

  /// Pop into 'out'.
  /// @return true on success; false if the ring is empty.
  bool try_pop(T& out) noexcept { return false; }

  /// Capacity in elements (power-of-two).
  std::size_t capacity() const noexcept { return capacity_; }

  /// Current number of elements.
  std::size_t size() const noexcept { return size_; }

  /// Convenience queries.
  bool empty() const noexcept { return size() == 0; }
  bool full() const noexcept { return size() == capacity(); }

private:
  const std::size_t capacity_;
  std::size_t mask_{0};
  std::size_t head_{0};
  std::size_t tail_{0};
  std::size_t size_{0};
};

} // namespace mpmc
