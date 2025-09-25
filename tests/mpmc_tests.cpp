#include "mpmc.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

// Detect TSan.
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define RING_TSAN 1
#endif
#endif
#if defined(__SANITIZE_THREAD__)
#define RING_TSAN 1
#endif

namespace {
#ifdef RING_TSAN
constexpr std::uint64_t kN = 250'000; // reduce workload under TSan
#else
constexpr std::uint64_t kN = 2'500'000;
#endif

constexpr std::size_t kCapacity = 64;
constexpr std::uint64_t kM_Backpress = 1024; // burn cadence
constexpr int kBurnIters = 500;              // burn intensity
constexpr std::chrono::seconds kRuntime = std::chrono::seconds(10);
constexpr std::size_t kNumProducers = 4;
constexpr std::size_t kNumConsumers = 4;

inline void burn_cycles() {
  volatile int sink = 0;
  for (int i = 0; i != kBurnIters; ++i)
    sink += i;
}
} // namespace

TEST(Ring, Construct) {
  mpmc::MpmcRing<int> ring(kCapacity);
  EXPECT_EQ(ring.capacity(), kCapacity);
  EXPECT_EQ(ring.size(), 0);
  EXPECT_TRUE(ring.empty());
  EXPECT_FALSE(ring.full());
}

/// Destructor must destroy all live elements (walk [tail, head)).
TEST(Ring, Destruct) {
  std::atomic<std::size_t> counter{0};
  struct CountingDestructor {
    std::atomic<std::size_t>* cnt{};
    ~CountingDestructor() { ++(*cnt); }
  };

  constexpr int capacity_int = static_cast<int>(kCapacity);
  {
    mpmc::MpmcRing<CountingDestructor> ring(kCapacity);
    for (int i = 0; i != capacity_int; ++i) {
      EXPECT_TRUE(ring.try_push(CountingDestructor{&counter}));
      --counter; // cancel out increment from temp destruction
    }
  }

  EXPECT_EQ(counter.load(), capacity_int);
}

TEST(Ring, Capacity) {
  // valid capacities
  EXPECT_NO_THROW(mpmc::MpmcRing<int> ring(2));
  EXPECT_NO_THROW(mpmc::MpmcRing<int> ring(16));
  EXPECT_NO_THROW(mpmc::MpmcRing<int> ring(65'536));

  // invalid capacities
  EXPECT_THROW(mpmc::MpmcRing<int> ring(1), std::invalid_argument);  // < 2
  EXPECT_THROW(mpmc::MpmcRing<int> ring(18), std::invalid_argument); // not power-of-two
}

/// Test smallest legal capacity.
TEST(Ring, CapacityTwo) {
  mpmc::MpmcRing<int> ring(2);
  int v;

  EXPECT_TRUE(ring.try_push(1));
  EXPECT_TRUE(ring.try_push(2));
  EXPECT_TRUE(ring.full());
  EXPECT_FALSE(ring.try_push(3));

  EXPECT_TRUE(ring.try_pop(v));
  EXPECT_EQ(v, 1);
  EXPECT_TRUE(ring.try_push(3));

  EXPECT_TRUE(ring.try_pop(v));
  EXPECT_EQ(v, 2);
  EXPECT_TRUE(ring.try_pop(v));
  EXPECT_EQ(v, 3);

  EXPECT_TRUE(ring.empty());
}

TEST(Ring, BasicPushPop) {
  constexpr int capacity_int = static_cast<int>(kCapacity);
  mpmc::MpmcRing<int> ring(kCapacity);

  // test basic push
  for (int i = 0; i != capacity_int; ++i) {
    EXPECT_TRUE(ring.try_push(i * i));
  }

  // test basic pop
  int v;
  for (int i = 0; i != capacity_int; ++i) {
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, i * i);
  }
}

TEST(Ring, FullEmptyBoundaries) {
  constexpr int capacity_int = static_cast<int>(kCapacity);
  mpmc::MpmcRing<int> ring(kCapacity);

  // test push boundaries
  for (int i = 0; i != capacity_int; ++i) {
    EXPECT_FALSE(ring.full());
    EXPECT_TRUE(ring.try_push(i * i));
  }
  EXPECT_TRUE(ring.full());
  EXPECT_FALSE(ring.try_push(999));

  // test pop boundaries
  int v;
  for (int i = 0; i != capacity_int; ++i) {
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_FALSE(ring.full());
  }
  EXPECT_TRUE(ring.empty());
  EXPECT_FALSE(ring.try_pop(v));
}

/// Exercise index wrap via bitmask; FIFO across wrap.
TEST(Ring, WrapAroundFifo) {
  constexpr int capacity_int = static_cast<int>(kCapacity);
  mpmc::MpmcRing<int> ring(kCapacity);

  // fill: 0..7
  for (int i = 0; i != capacity_int; ++i) {
    EXPECT_TRUE(ring.try_push(i));
  }
  EXPECT_TRUE(ring.full());
  EXPECT_FALSE(ring.try_push(999));

  // pop half: 0..3
  int v;
  for (int i = 0; i != capacity_int / 2; ++i) {
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, i);
  }
  EXPECT_FALSE(ring.full());

  // refill: 8..11 (forces wrap)
  for (int i = 0; i != capacity_int / 2; ++i) {
    EXPECT_TRUE(ring.try_push(capacity_int + i));
  }
  EXPECT_TRUE(ring.full());
  EXPECT_FALSE(ring.try_push(999));

  // drain: 4..11 (FIFO across wrap)
  for (int i = 0; i != capacity_int; ++i) {
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, i + capacity_int / 2);
  }
  EXPECT_TRUE(ring.empty());
}

/// Move-only payload.
TEST(Ring, MoveOnlyType) {
  mpmc::MpmcRing<std::unique_ptr<int>> ring(kCapacity);

  // initialize values
  std::vector<std::unique_ptr<int>> vals;
  vals.reserve(kCapacity);
  for (std::size_t i = 0; i != kCapacity; ++i) {
    vals.emplace_back(std::make_unique<int>(static_cast<int>(i)));
  }

  // push by move
  for (std::size_t i = 0; i != kCapacity; ++i) {
    EXPECT_TRUE(ring.try_push(std::move(vals[i])));
    EXPECT_EQ(vals[i], nullptr); // source becomes null
  }
  EXPECT_TRUE(ring.full());

  // pop and verify FIFO
  std::unique_ptr<int> out;
  for (std::size_t i = 0; i != kCapacity; ++i) {
    ASSERT_TRUE(ring.try_pop(out));
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(*out, static_cast<int>(i));
  }
  EXPECT_TRUE(ring.empty());
}

/// Validate SPSC publish/observe ordering.
TEST(RingSPSC, BasicPushPop) {
  const auto deadline = std::chrono::steady_clock::now() + kRuntime;
  mpmc::MpmcRing<std::uint64_t> ring(kCapacity);
  std::uint64_t produced_count = 0;
  std::uint64_t consumed_count = 0;

  std::thread producer([&] {
    for (std::uint64_t i = 0; i != kN; ++i) {
      while (!ring.try_push(i)) {
        if (std::chrono::steady_clock::now() > deadline) {
          ADD_FAILURE() << "Producer timeout";
          return;
        }
        std::this_thread::yield();
      }
      ++produced_count;
    }
  });

  std::thread consumer([&] {
    for (std::uint64_t i = 0; i != kN; ++i) {
      std::uint64_t out;
      while (!ring.try_pop(out)) {
        if (std::chrono::steady_clock::now() > deadline) {
          ADD_FAILURE() << "Consumer timeout";
          return;
        }
        std::this_thread::yield();
      }
      ASSERT_EQ(out, i);
      ++consumed_count;
    }
  });

  producer.join();
  consumer.join();

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(produced_count, kN);
  EXPECT_EQ(consumed_count, kN);
}

/// Test SPSC backpressure caused by slowed producer.
TEST(RingSPSC, BackpressureConsumerFaster) {
  const auto deadline = std::chrono::steady_clock::now() + kRuntime;
  mpmc::MpmcRing<std::uint64_t> ring(kCapacity);
  std::uint64_t produced_count = 0;
  std::uint64_t consumed_count = 0;

  std::thread producer([&] {
    for (std::uint64_t i = 0; i != kN; ++i) {
      if (i % kM_Backpress == 0) {
        burn_cycles();
      }
      while (!ring.try_push(i)) {
        if (std::chrono::steady_clock::now() > deadline) {
          ADD_FAILURE() << "Producer timeout";
          return;
        }
        std::this_thread::yield();
      }
      ++produced_count;
    }
  });

  std::thread consumer([&] {
    for (std::uint64_t i = 0; i != kN; ++i) {
      std::uint64_t out;
      while (!ring.try_pop(out)) {
        if (std::chrono::steady_clock::now() > deadline) {
          ADD_FAILURE() << "Consumer timeout";
          return;
        }
        std::this_thread::yield();
      }
      ASSERT_EQ(out, i);
      ++consumed_count;
    }
  });

  producer.join();
  consumer.join();

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(produced_count, kN);
  EXPECT_EQ(consumed_count, kN);
}

/// Test SPSC backpressure caused by slowed consumer.
TEST(RingSPSC, BackpressureProducerFaster) {
  const auto deadline = std::chrono::steady_clock::now() + kRuntime;
  mpmc::MpmcRing<std::uint64_t> ring(kCapacity);
  std::uint64_t produced_count = 0;
  std::uint64_t consumed_count = 0;

  std::thread producer([&] {
    for (std::uint64_t i = 0; i != kN; ++i) {
      while (!ring.try_push(i)) {
        if (std::chrono::steady_clock::now() > deadline) {
          ADD_FAILURE() << "Producer timeout";
          return;
        }
        std::this_thread::yield();
      }
      ++produced_count;
    }
  });

  std::thread consumer([&] {
    for (std::uint64_t i = 0; i != kN; ++i) {
      if (i % kM_Backpress == 0) {
        burn_cycles();
      }
      std::uint64_t out;
      while (!ring.try_pop(out)) {
        if (std::chrono::steady_clock::now() > deadline) {
          ADD_FAILURE() << "Consumer timeout";
          return;
        }
        std::this_thread::yield();
      }
      ASSERT_EQ(out, i);
      ++consumed_count;
    }
  });

  producer.join();
  consumer.join();

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(produced_count, kN);
  EXPECT_EQ(consumed_count, kN);
}

/// Move-only payload across threads (SPSC).
TEST(RingSPSC, MoveOnlyType) {
  const auto deadline = std::chrono::steady_clock::now() + kRuntime;
  mpmc::MpmcRing<std::unique_ptr<std::uint64_t>> ring(kCapacity);
  std::uint64_t produced_count = 0;
  std::uint64_t consumed_count = 0;

  std::thread producer([&] {
    for (std::uint64_t i = 0; i != kN; ++i) {
      while (true) {
        auto p = std::make_unique<std::uint64_t>(i);
        if (ring.try_push(std::move(p))) {
          EXPECT_EQ(p, nullptr); // source was moved-from
          break;
        }
        if (std::chrono::steady_clock::now() > deadline) {
          ADD_FAILURE() << "Producer timeout";
          return;
        }
        std::this_thread::yield();
      }
      ++produced_count;
    }
  });

  std::thread consumer([&] {
    for (std::uint64_t i = 0; i != kN; ++i) {
      std::unique_ptr<std::uint64_t> out;
      while (!ring.try_pop(out)) {
        if (std::chrono::steady_clock::now() > deadline) {
          ADD_FAILURE() << "Consumer timeout";
          return;
        }
        std::this_thread::yield();
      }
      ASSERT_NE(out, nullptr);
      EXPECT_EQ(*out, i);
      ++consumed_count;
    }
  });

  producer.join();
  consumer.join();

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(produced_count, kN);
  EXPECT_EQ(consumed_count, kN);
}

/// Validate MPMC publish/observe ordering.
TEST(RingMPMC, DISABLED_BasicPushPop) {
  const auto deadline = std::chrono::steady_clock::now() + kRuntime;
  mpmc::MpmcRing<std::uint64_t> ring(kCapacity);

  std::atomic<std::uint64_t> produced_count = 0;
  std::vector<std::thread> producers;
  for (std::size_t i = 0; i != kNumProducers; ++i) {
    producers.emplace_back([&, i] {
      for (std::uint64_t j = i; j < kN; j += kNumProducers) {
        while (!ring.try_push(j)) {
          if (std::chrono::steady_clock::now() > deadline) {
            ADD_FAILURE() << "Producer timeout";
            return;
          }
          std::this_thread::yield();
        }
        produced_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::vector<std::atomic_bool> is_seen(kN);
  for (auto& s : is_seen) {
    s.store(false, std::memory_order_relaxed);
  }
  std::atomic<std::uint64_t> consumed_count = 0;
  std::vector<std::thread> consumers;
  for (std::size_t i = 0; i != kNumConsumers; ++i) {
    consumers.emplace_back([&, i] {
      for (std::uint64_t j = i; j < kN; j += kNumConsumers) {
        std::uint64_t out;
        while (!ring.try_pop(out)) {
          if (std::chrono::steady_clock::now() > deadline) {
            ADD_FAILURE() << "Consumer timeout";
            return;
          }
          std::this_thread::yield();
        }
        ASSERT_LT(out, kN);
        const auto prev = is_seen[out].exchange(true, std::memory_order_relaxed);
        ASSERT_FALSE(prev);
        consumed_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (std::size_t i = 0; i != kNumProducers; ++i) {
    producers[i].join();
  }
  for (std::size_t i = 0; i != kNumConsumers; ++i) {
    consumers[i].join();
  }

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(produced_count.load(std::memory_order_relaxed), kN);
  EXPECT_EQ(consumed_count.load(std::memory_order_relaxed), kN);

  for (std::uint64_t i = 0; i != kN; ++i) {
    EXPECT_TRUE(is_seen[i].load(std::memory_order_relaxed));
  }
}

/// Test MPMC backpressure caused by slowed producer.
TEST(RingMPMC, DISABLED_BackpressureConsumerFaster) {
  const auto deadline = std::chrono::steady_clock::now() + kRuntime;
  mpmc::MpmcRing<std::uint64_t> ring(kCapacity);

  std::atomic<std::uint64_t> produced_count = 0;
  std::vector<std::thread> producers;
  for (std::size_t i = 0; i != kNumProducers; ++i) {
    producers.emplace_back([&, i] {
      for (std::uint64_t j = i; j < kN; j += kNumProducers) {
        if (j % kM_Backpress == 0) {
          burn_cycles();
        }
        while (!ring.try_push(j)) {
          if (std::chrono::steady_clock::now() > deadline) {
            ADD_FAILURE() << "Producer timeout";
            return;
          }
          std::this_thread::yield();
        }
        produced_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::vector<std::atomic_bool> is_seen(kN);
  for (auto& s : is_seen) {
    s.store(false, std::memory_order_relaxed);
  }
  std::atomic<std::uint64_t> consumed_count = 0;
  std::vector<std::thread> consumers;
  for (std::size_t i = 0; i != kNumConsumers; ++i) {
    consumers.emplace_back([&, i] {
      for (std::uint64_t j = i; j < kN; j += kNumConsumers) {
        std::uint64_t out;
        while (!ring.try_pop(out)) {
          if (std::chrono::steady_clock::now() > deadline) {
            ADD_FAILURE() << "Consumer timeout";
            return;
          }
          std::this_thread::yield();
        }
        ASSERT_LT(out, kN);
        const auto prev = is_seen[out].exchange(true, std::memory_order_relaxed);
        ASSERT_FALSE(prev);
        consumed_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (std::size_t i = 0; i != kNumProducers; ++i) {
    producers[i].join();
  }
  for (std::size_t i = 0; i != kNumConsumers; ++i) {
    consumers[i].join();
  }

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(produced_count.load(std::memory_order_relaxed), kN);
  EXPECT_EQ(consumed_count.load(std::memory_order_relaxed), kN);

  for (std::uint64_t i = 0; i != kN; ++i) {
    EXPECT_TRUE(is_seen[i].load(std::memory_order_relaxed));
  }
}

/// Test MPMC backpressure caused by slowed consumer.
TEST(RingMPMC, DISABLED_BackpressureProducerFaster) {
  const auto deadline = std::chrono::steady_clock::now() + kRuntime;
  mpmc::MpmcRing<std::uint64_t> ring(kCapacity);

  std::atomic<std::uint64_t> produced_count = 0;
  std::vector<std::thread> producers;
  for (std::size_t i = 0; i != kNumProducers; ++i) {
    producers.emplace_back([&, i] {
      for (std::uint64_t j = i; j < kN; j += kNumProducers) {
        while (!ring.try_push(j)) {
          if (std::chrono::steady_clock::now() > deadline) {
            ADD_FAILURE() << "Producer timeout";
            return;
          }
          std::this_thread::yield();
        }
        produced_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::vector<std::atomic_bool> is_seen(kN);
  for (auto& s : is_seen) {
    s.store(false, std::memory_order_relaxed);
  }
  std::atomic<std::uint64_t> consumed_count = 0;
  std::vector<std::thread> consumers;
  for (std::size_t i = 0; i != kNumConsumers; ++i) {
    consumers.emplace_back([&, i] {
      for (std::uint64_t j = i; j < kN; j += kNumConsumers) {
        std::uint64_t out;
        if (j % kM_Backpress == 0) {
          burn_cycles();
        }
        while (!ring.try_pop(out)) {
          if (std::chrono::steady_clock::now() > deadline) {
            ADD_FAILURE() << "Consumer timeout";
            return;
          }
          std::this_thread::yield();
        }
        ASSERT_LT(out, kN);
        const auto prev = is_seen[out].exchange(true, std::memory_order_relaxed);
        ASSERT_FALSE(prev);
        consumed_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (std::size_t i = 0; i != kNumProducers; ++i) {
    producers[i].join();
  }
  for (std::size_t i = 0; i != kNumConsumers; ++i) {
    consumers[i].join();
  }

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(produced_count.load(std::memory_order_relaxed), kN);
  EXPECT_EQ(consumed_count.load(std::memory_order_relaxed), kN);

  for (std::uint64_t i = 0; i != kN; ++i) {
    EXPECT_TRUE(is_seen[i].load(std::memory_order_relaxed));
  }
}

/// Move-only payload across threads (MPMC).
TEST(RingMPMC, DISABLED_MoveOnlyType) {
  const auto deadline = std::chrono::steady_clock::now() + kRuntime;
  mpmc::MpmcRing<std::unique_ptr<std::uint64_t>> ring(kCapacity);

  std::atomic<std::uint64_t> produced_count = 0;
  std::vector<std::thread> producers;
  for (std::size_t i = 0; i != kNumProducers; ++i) {
    producers.emplace_back([&, i] {
      for (std::uint64_t j = i; j < kN; j += kNumProducers) {
        while (true) {
          auto p = std::make_unique<std::uint64_t>(j);
          if (ring.try_push(std::move(p))) {
            EXPECT_EQ(p, nullptr); // source was moved-from
            break;
          }
          if (std::chrono::steady_clock::now() > deadline) {
            ADD_FAILURE() << "Producer timeout";
            return;
          }
          std::this_thread::yield();
        }
        produced_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::vector<std::atomic_bool> is_seen(kN);
  for (auto& s : is_seen) {
    s.store(false, std::memory_order_relaxed);
  }
  std::atomic<std::uint64_t> consumed_count = 0;
  std::vector<std::thread> consumers;
  for (std::size_t i = 0; i != kNumConsumers; ++i) {
    consumers.emplace_back([&, i] {
      for (std::uint64_t j = i; j < kN; j += kNumConsumers) {
        std::unique_ptr<std::uint64_t> out;
        while (!ring.try_pop(out)) {
          if (std::chrono::steady_clock::now() > deadline) {
            ADD_FAILURE() << "Consumer timeout";
            return;
          }
          std::this_thread::yield();
        }
        ASSERT_NE(out, nullptr);
        ASSERT_LT(*out, kN);
        const auto prev = is_seen[*out].exchange(true, std::memory_order_relaxed);
        ASSERT_FALSE(prev);
        consumed_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (std::size_t i = 0; i != kNumProducers; ++i) {
    producers[i].join();
  }
  for (std::size_t i = 0; i != kNumConsumers; ++i) {
    consumers[i].join();
  }

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(produced_count.load(std::memory_order_relaxed), kN);
  EXPECT_EQ(consumed_count.load(std::memory_order_relaxed), kN);

  for (std::uint64_t i = 0; i != kN; ++i) {
    EXPECT_TRUE(is_seen[i].load(std::memory_order_relaxed));
  }
}