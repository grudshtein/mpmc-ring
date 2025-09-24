#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <algorithm>

namespace mpmc {

// Platform/ABI guarantees for this ring:
static_assert(sizeof(void*) == 8, "64-bit platform required");
static_assert(sizeof(std::size_t) == 8, "64-bit size_t required");
static_assert(std::atomic<std::size_t>::is_always_lock_free,
              "size_t atomics must be lock-free on this platform");

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

  /// Destroy the live range [tail_, tail_ + size()) then free aligned storage.
  ~MpmcRing() {
    if constexpr (!std::is_trivially_destructible_v<T>) {
      const auto h = head_.load(std::memory_order_relaxed);
      const auto t = tail_.load(std::memory_order_relaxed);
      for (auto idx = t; idx != h; ++idx) {
        std::destroy_at(buffer_ + slot(idx));
      }
    }
    if (buffer_) {
      ::operator delete[](buffer_, std::align_val_t{alignof(T)});
    }
  }

  /// Push by copy.
  /// @return true on success; false if the ring is full.
  [[nodiscard]] bool try_push(const T& v) noexcept(std::is_nothrow_copy_constructible_v<T>) {
    const auto h = head_.load(std::memory_order_relaxed);
    const auto t = tail_.load(std::memory_order_acquire);
    if (h - t == capacity()) {
      return false;
    }
    std::construct_at(buffer_ + slot(h), v);
    head_.store(h + 1, std::memory_order_release);
    return true;
  }

  /// Push by move.
  /// @return true on success; false if the ring is full.
  [[nodiscard]] bool try_push(T&& v) noexcept(std::is_nothrow_move_constructible_v<T>) {
    const auto h = head_.load(std::memory_order_relaxed);
    const auto t = tail_.load(std::memory_order_acquire);
    if (h - t == capacity()) {
      return false;
    }
    std::construct_at(buffer_ + slot(h), std::move(v));
    head_.store(h + 1, std::memory_order_release);
    return true;
  }

  /// Pop into 'out'.
  /// @return true on success; false if the ring is empty.
  [[nodiscard]] bool try_pop(T& out) noexcept(std::is_nothrow_move_assignable_v<T>) {
    const auto h = head_.load(std::memory_order_acquire);
    const auto t = tail_.load(std::memory_order_relaxed);
    if (h == t) {
      return false;
    }
    out = std::move(buffer_[slot(t)]);
    std::destroy_at(buffer_ + slot(t));
    tail_.store(t + 1, std::memory_order_release);
    return true;
  }

  /// Capacity in elements (power-of-two).
  [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

  /// Convenience queries. Advisory, exact only in quiescent states.
  [[nodiscard]] std::size_t size() const noexcept {
    const auto h = head_.load(std::memory_order_relaxed);
    const auto t = tail_.load(std::memory_order_relaxed);
    return std::min(h - t, capacity_);  // clamped to [0, capacity]
  }
  [[nodiscard]] bool empty() const noexcept { return size() == 0; }
  [[nodiscard]] bool full() const noexcept { return size() == capacity(); }

private:
  const std::size_t capacity_;
  const std::size_t mask_;
  T* buffer_;
  std::atomic<std::size_t> head_{0};
  std::atomic<std::size_t> tail_{0};

  /// Validates the fixed ring size. Requirements: c >= 2 and c is a power of two.
  [[nodiscard]] static std::size_t validate_capacity(std::size_t c) {
    if (c < 2)
      throw std::invalid_argument("capacity must be >= 2");
    if ((c & (c - 1)) != 0)
      throw std::invalid_argument("capacity must be a power of 2");
    return c;
  }

  /// Maps an index to a physical slot in [0, capacity).
  [[nodiscard]] std::size_t slot(const std::size_t index) const noexcept { return index & mask_; }
};

} // namespace mpmc
