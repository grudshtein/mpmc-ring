#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define CPU_PAUSE() _mm_pause()
#elif defined(__aarch64__) || defined(__arm__)
#include <arm_acle.h>
#define CPU_PAUSE() __yield()
#else
#define CPU_PAUSE() ((void)0)
#endif

namespace mpmc {

namespace detail {

inline constexpr std::size_t kCacheLine = 64;

template <bool Padding>
struct Atomic64 : std::atomic<std::uint64_t> {
  using base = std::atomic<std::uint64_t>;
  using base::base;
};

template <>
struct alignas(kCacheLine) Atomic64<true> : std::atomic<std::uint64_t> {
  using base = std::atomic<std::uint64_t>;
  using base::base;
};

// verify padded atomic is cache-line aligned and sized
static_assert(alignof(Atomic64<true>) >= kCacheLine, "padded atomic must be cache-line aligned");
static_assert(sizeof(Atomic64<true>) % kCacheLine == 0,
              "padded atomic must be a multiple of cache line");

} // namespace detail

// Platform/ABI guarantees for this ring:
static_assert(sizeof(void*) == 8, "64-bit platform required");
static_assert(sizeof(std::size_t) == 8, "64-bit size_t required");
static_assert(std::atomic<std::size_t>::is_always_lock_free,
              "size_t atomics must be lock-free on this platform");
static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
              "uint64_t atomics must be lock-free on this platform");

// Lock-free bounded MPMC ring for arbitrary T.
template <typename T, bool Padding = false>
class MpmcRing {
public:
  /// Construct a fixed-capacity ring.
  /// @param capacity Must be power-of-two and >= 2; fixed for object lifetime.
  /// @throws std::invalid_argument if precondition is violated.
  explicit MpmcRing(std::size_t capacity)
      : capacity_{validate_capacity(capacity)}, mask_{static_cast<std::uint64_t>(capacity_) - 1},
        buffer_{nullptr} {
    void* mem = ::operator new[](sizeof(Slot) * capacity_, std::align_val_t{alignof(Slot)});
    buffer_ = static_cast<Slot*>(mem);
    for (std::size_t i = 0; i != capacity_; ++i) {
      buffer_[i].code_.store(static_cast<std::uint64_t>(i), std::memory_order_relaxed);
    }
  }

  /// Non-copyable, non-movable: owns raw storage. (Avoid double-free / shallow copies.)
  MpmcRing() = delete;
  MpmcRing(const MpmcRing& other) = delete;
  MpmcRing& operator=(const MpmcRing&) = delete;
  MpmcRing(MpmcRing&& other) = delete;
  MpmcRing& operator=(MpmcRing&&) = delete;

  /// Destroy the live range [tail_, tail_ + size()) then free aligned storage.
  ~MpmcRing() {
    // destroy elements
    if constexpr (!std::is_trivially_destructible_v<T>) {
      const auto head = head_.load(std::memory_order_relaxed);
      const auto tail = tail_.load(std::memory_order_relaxed);
      for (std::uint64_t idx = tail; idx != head; ++idx) {
        auto slot = get_slot(idx);
        const auto code = slot->code_.load(std::memory_order_acquire);
        if (code != idx + 1) {
          continue; // only destroy live elements
        }
        std::destroy_at(get_data(slot));
      }
    }

    // destroy buffer
    if (buffer_) {
      ::operator delete[](buffer_, std::align_val_t{alignof(Slot)});
    }
  }

  /// Non-blocking push by copy.
  /// @return true on success; false if the ring is full.
  [[nodiscard]] bool try_push(const T& v) noexcept(std::is_nothrow_copy_constructible_v<T>) {
    while (true) {
      auto ticket = head_.load(std::memory_order_relaxed);
      auto slot = get_slot(ticket);
      auto code = slot->code_.load(std::memory_order_acquire);
      const auto diff = static_cast<int64_t>(code) - static_cast<int64_t>(ticket);

      if (diff > 0) {
        continue; // stale: retry
      } else if (diff < 0) {
        return false; // slot full: return
      } else {
        // claim the ticket
        if (!head_.compare_exchange_weak(ticket, ticket + 1, std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
          continue; // contention: retry
        } else {
          std::construct_at(get_data(slot), v);
          slot->code_.store(ticket + 1, std::memory_order_release);
          return true;
        }
      }
    }
  }

  /// Push by copy.
  void push(const T& v) noexcept(std::is_nothrow_copy_constructible_v<T>) {
    const auto ticket = head_.fetch_add(1, std::memory_order_relaxed); // claim the ticket
    auto* slot = get_slot(ticket);
    while (true) { // wait until slot is free for this ticket
      const auto code = slot->code_.load(std::memory_order_acquire);
      if (code == ticket) {
        break;
      }
      CPU_PAUSE();
    }
    std::construct_at(get_data(slot), v);
    slot->code_.store(ticket + 1, std::memory_order_release);
  }

  /// Non-blocking push by move.
  /// @return true on success; false if the ring is full.
  [[nodiscard]] bool try_push(T&& v) noexcept(std::is_nothrow_move_constructible_v<T>) {
    while (true) {
      auto ticket = head_.load(std::memory_order_relaxed);
      auto slot = get_slot(ticket);
      auto code = slot->code_.load(std::memory_order_acquire);
      const auto diff = static_cast<int64_t>(code) - static_cast<int64_t>(ticket);

      if (diff > 0) {
        continue; // stale: retry
      } else if (diff < 0) {
        return false; // slot full: return
      } else {
        // claim the ticket
        if (!head_.compare_exchange_weak(ticket, ticket + 1, std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
          continue; // contention: retry
        } else {
          std::construct_at(get_data(slot), std::move(v));
          slot->code_.store(ticket + 1, std::memory_order_release);
          return true;
        }
      }
    }
  }

  /// Push by move.
  void push(T&& v) noexcept(std::is_nothrow_move_constructible_v<T>) {
    const auto ticket = head_.fetch_add(1, std::memory_order_relaxed); // claim the ticket
    auto* slot = get_slot(ticket);
    while (true) { // wait until slot is free for this ticket
      const auto code = slot->code_.load(std::memory_order_acquire);
      if (code == ticket) {
        break;
      }
      CPU_PAUSE();
    }
    std::construct_at(get_data(slot), std::move(v));
    slot->code_.store(ticket + 1, std::memory_order_release);
  }

  /// Non-blocking pop into 'out'.
  /// @return true on success; false if the ring is empty.
  [[nodiscard]] bool try_pop(T& out) noexcept(std::is_nothrow_move_assignable_v<T>) {
    while (true) {
      auto ticket = tail_.load(std::memory_order_relaxed);
      auto slot = get_slot(ticket);
      auto code = slot->code_.load(std::memory_order_acquire);
      const auto diff = static_cast<int64_t>(code) - static_cast<int64_t>(ticket + 1);

      if (diff > 0) {
        continue; // stale: retry
      } else if (diff < 0) {
        return false; // slot empty: return
      } else {
        // claim the ticket
        if (!tail_.compare_exchange_weak(ticket, ticket + 1, std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
          continue; // contention: retry
        } else {
          out = std::move(*get_data(slot));
          std::destroy_at(get_data(slot));
          slot->code_.store(ticket + static_cast<std::uint64_t>(capacity_),
                            std::memory_order_release);
          return true;
        }
      }
    }
  }

  /// Pop into out.
  void pop(T& out) noexcept(std::is_nothrow_move_assignable_v<T>) {
    const auto ticket = tail_.fetch_add(1, std::memory_order_relaxed); // claim the ticket
    auto* slot = get_slot(ticket);
    while (true) { // wait until slot is ready for reading
      const auto code = slot->code_.load(std::memory_order_acquire);
      if (code == ticket + 1) {
        break;
      }
      CPU_PAUSE();
    }
    out = std::move(*get_data(slot));
    std::destroy_at(get_data(slot));
    slot->code_.store(ticket + static_cast<std::uint64_t>(capacity_), std::memory_order_release);
  }

  /// Capacity in elements (power-of-two).
  [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

  /// Convenience queries. Advisory, exact only in quiescent states.
  [[nodiscard]] std::size_t size() const noexcept {
    const auto head = head_.load(std::memory_order_relaxed);
    const auto tail = tail_.load(std::memory_order_relaxed);
    const std::size_t size = head - tail;
    return std::min(size, capacity_); // clamped to [0, capacity]
  }
  [[nodiscard]] bool empty() const noexcept { return size() == 0; }
  [[nodiscard]] bool full() const noexcept { return size() == capacity(); }

private:
  struct Slot {
    std::atomic<std::uint64_t> code_;
    std::aligned_storage_t<sizeof(T), alignof(T)> storage_;
  };

  const std::size_t capacity_;
  const std::uint64_t mask_;
  Slot* buffer_;
  detail::Atomic64<Padding> head_{0};
  detail::Atomic64<Padding> tail_{0};

  // verify head_ and tail_ alignment
  static_assert(!Padding || alignof(decltype(head_)) >= detail::kCacheLine,
                "head_ must be cache-line aligned when using padding");
  static_assert(!Padding || alignof(decltype(tail_)) >= detail::kCacheLine,
                "tail_ must be cache-line aligned when using padding");

  /// Validates the fixed ring size. Requirements: c >= 2 and c is a power of two.
  [[nodiscard]] static std::size_t validate_capacity(std::size_t c) {
    if (c < 2)
      throw std::invalid_argument("capacity must be >= 2");
    if ((c & (c - 1)) != 0)
      throw std::invalid_argument("capacity must be a power of 2");
    return c;
  }

  /// Maps an index to a physical slot in [0, capacity).
  [[nodiscard]] Slot* get_slot(const std::uint64_t index) const noexcept {
    return &buffer_[index & mask_];
  }

  /// Return a pointer to the T object constructed in slot->storage_.
  [[nodiscard]] static T* get_data(Slot* slot) noexcept {
    return std::launder(reinterpret_cast<T*>(&slot->storage_));
  }
  [[nodiscard]] static const T* get_data(const Slot* slot) noexcept {
    return std::launder(reinterpret_cast<const T*>(&slot->storage_));
  }
};

} // namespace mpmc
