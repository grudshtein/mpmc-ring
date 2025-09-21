#pragma once

#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace mpmc {

// Single-threaded bounded ring for arbitrary T.
template <typename T>
class MpmcRing {
public:
  /// Construct a fixed-capacity ring.
  /// @param capacity Must be power-of-two and >= 2; fixed for object lifetime.
  /// @throws std::invalid_argument if precondition is violated.
  explicit MpmcRing(std::size_t capacity)
      : capacity_{validate_capacity(capacity)}, mask_{capacity_ - 1}, buffer_{nullptr} {
    void* mem = ::operator new[](sizeof(T) * capacity_, std::align_val_t{alignof(T)});
    buffer_ = static_cast<T*>(mem);
  }

  /// Non-copyable, non-movable: owns raw storage. (Avoid double-free / shallow copies.)
  MpmcRing() = delete;
  MpmcRing(const MpmcRing& other) = delete;
  MpmcRing& operator=(const MpmcRing&) = delete;
  MpmcRing(MpmcRing&& other) = delete;
  MpmcRing& operator=(MpmcRing&&) = delete;

  /// Destroy the live range [tail_, tail_ + size_) then free aligned storage.
  ~MpmcRing() {
    if constexpr (!std::is_trivially_destructible_v<T>) {
      for (auto i = 0; i != size_; ++i) {
        std::destroy_at(buffer_ + slot(tail_ + i));
      }
    }
    if (buffer_) {
      ::operator delete[](buffer_, std::align_val_t{alignof(T)});
    }
  }

  /// Push by copy.
  /// @return true on success; false if the ring is full.
  [[nodiscard]] bool try_push(const T& v) noexcept(std::is_nothrow_copy_constructible_v<T>) {
    if (full()) {
      return false;
    }
    std::construct_at(buffer_ + slot(head_), v);
    ++head_;
    ++size_;
    return true;
  }

  /// Push by move.
  /// @return true on success; false if the ring is full.
  [[nodiscard]] bool try_push(T&& v) noexcept(std::is_nothrow_move_constructible_v<T>) {
    if (full()) {
      return false;
    }
    std::construct_at(buffer_ + slot(head_), std::move(v));
    ++head_;
    ++size_;
    return true;
  }

  /// Pop into 'out'.
  /// @return true on success; false if the ring is empty.
  [[nodiscard]] bool try_pop(T& out) noexcept(std::is_nothrow_move_assignable_v<T>) {
    if (empty()) {
      return false;
    }
    out = std::move(buffer_[slot(tail_)]);
    std::destroy_at(buffer_ + slot(tail_));
    ++tail_;
    --size_;
    return true;
  }

  /// Capacity in elements (power-of-two).
  std::size_t capacity() const noexcept { return capacity_; }

  /// Current number of elements.
  std::size_t size() const noexcept { return size_; }

  /// Convenience queries.
  bool empty() const noexcept { return size() == 0; }
  bool full() const noexcept { return size() == capacity(); }

private:
  const std::size_t capacity_;
  const std::size_t mask_;
  T* buffer_;
  std::size_t head_{0};
  std::size_t tail_{0};
  std::size_t size_{0};

  static std::size_t validate_capacity(std::size_t c) {
    if (c < 2)
      throw std::invalid_argument("capacity must be >= 2");
    if ((c & (c - 1)) != 0)
      throw std::invalid_argument("capacity must be a power of 2");
    return c;
  }

  std::size_t slot(const std::size_t index) const noexcept { return index & mask_; }
};

} // namespace mpmc
