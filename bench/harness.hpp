#include "mpmc.hpp"

#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <immintrin.h>
#include <iostream>
#include <numeric>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

// Platform-specific includes required for thread affinity support.
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>

#elif defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

#else
// no thread affinity support on this platform
#endif

#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
#include <x86intrin.h>
#endif

namespace mpmc::bench {

// Pin the current thread
inline void set_thread_affinity_current(int core_id) {
#if defined(__linux__)
  // Linux implementation (pthread_setaffinity_np)
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(static_cast<size_t>(core_id), &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    throw std::runtime_error("pthread_setaffinity_np failed (self)");
  }
#elif defined(_WIN32)
  // Windows implementation (SetThreadGroupAffinity)
  if (core_id < 0) {
    throw std::invalid_argument("core_id must be non-negative");
  }
  GROUP_AFFINITY affinity = {};
  affinity.Group = static_cast<WORD>(core_id / 64);
  affinity.Mask = 1ull << (core_id % 64);
  if (!SetThreadGroupAffinity(GetCurrentThread(), &affinity, nullptr)) {
    throw std::runtime_error("SetThreadGroupAffinity failed");
  }
#else
  (void)core_id; // no-op on unsupported platforms
#endif
}

// Get the current value of the processor's cycle counter
inline uint64_t read_tsc() noexcept {
#if defined(_MSC_VER)
  return __rdtsc();
#elif defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
  unsigned int aux;
  return __rdtscp(&aux);
#else
  // fallback
  return std::chrono::steady_clock::now().time_since_epoch().count();
#endif
}

constexpr int SAMPLE_RATE = 100; // rate at which to update histogram

struct Config {
  std::size_t num_producers{1};
  std::size_t num_consumers{1};
  std::size_t capacity{65'536};
  bool blocking{true};
  std::chrono::milliseconds duration_ms{17'500};
  std::chrono::milliseconds warmup_ms{2'500};
  std::chrono::nanoseconds histogram_bucket_width{5};
  std::size_t histogram_max_buckets{4'096};
  bool pinning_on{true};
  bool padding_on{true};
  bool large_payload{false};
  bool move_only_payload{false};
  std::string csv_path{"results/raw/results.csv"};
  std::string notes{""};
};

struct Results {
  struct LatencyStats {
    std::chrono::nanoseconds min{std::chrono::nanoseconds::max()};
    std::chrono::nanoseconds p50{0};
    std::chrono::nanoseconds p95{0};
    std::chrono::nanoseconds p99{0};
    std::chrono::nanoseconds p999{0};
    std::chrono::nanoseconds max{0};
    std::chrono::nanoseconds mean{0};
    uint64_t spikes_over_10x_p50{0}; // tail spikes
  };

  // metadata
  const Config& config;
  std::chrono::nanoseconds wall_time{}; // excludes warmup

  // throughput
  uint64_t pushes_ok{};
  uint64_t pops_ok{};
  uint64_t try_push_failures{}; // ring full
  uint64_t try_pop_failures{};  // ring empty

  // latencies (ns)
  LatencyStats push_latencies;
  LatencyStats pop_latencies;

  // histogram
  std::vector<uint64_t> push_histogram{}; // push counts per bucket
  std::vector<uint64_t> pop_histogram{};  // pop counts per bucket
  uint64_t push_overflows{};              // push latencies which overflowed histogram range
  uint64_t pop_overflows{};               // pop latencies which overflowed histogram range

  // notes for reproducibility
  std::string notes{};

  // constructor
  explicit Results(const Config& cfg) : config(cfg) {}

  // convenience functions
  [[nodiscard]] inline double push_ops_per_sec() const {
    const double secs = std::chrono::duration<double>(wall_time).count();
    return secs > 0 ? static_cast<double>(pushes_ok) / secs : 0.0;
  }
  [[nodiscard]] inline double pop_ops_per_sec() const {
    const double secs = std::chrono::duration<double>(wall_time).count();
    return secs > 0 ? static_cast<double>(pops_ok) / secs : 0.0;
  }
  void combine(const Results& other);
  void set_latencies(LatencyStats& latencies, const std::vector<uint64_t>& histogram);

  // CSV functions
  void append_csv() const;
  static void write_csv_header(std::ostream& os);
  void write_csv_row(std::ostream& os) const;
  static std::string escape_csv(std::string_view s);
};

class Harness {
public:
  explicit Harness(const Config& config) : config_{config} {}
  inline Results run_once() const {
    if (config_.large_payload) {
      if (config_.move_only_payload) {
        return run_once<std::unique_ptr<std::array<uint64_t, 128>>>();
      } else {
        return run_once<std::array<uint64_t, 128>>();
      }
    } else {
      if (config_.move_only_payload) {
        return run_once<std::unique_ptr<uint64_t>>();
      } else {
        return run_once<uint64_t>();
      }
    }
  }

private:
  const Config& config_;

  template <typename T>
  Results run_once() const {
    if (config_.padding_on) {
      return run_once_impl<T, true>();
    } else {
      return run_once_impl<T, false>();
    }
  }

  template <typename T, bool Padding>
  Results run_once_impl() const {
    MpmcRing<T, Padding> ring(config_.capacity);
    std::atomic<bool> collecting{false};
    std::atomic<bool> done{false};

    Results results{config_};
    results.push_histogram.resize(config_.histogram_max_buckets, 0);
    results.pop_histogram.resize(config_.histogram_max_buckets, 0);

    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    std::vector<Results> producer_results(config_.num_producers, results);
    std::vector<Results> consumer_results(config_.num_consumers, results);

    // measure nanoseconds per cycle
    double ns_per_cycle;
    {
      const auto t0 = std::chrono::steady_clock::now();
      const uint64_t c0 = read_tsc();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      const uint64_t c1 = read_tsc();
      const auto t1 = std::chrono::steady_clock::now();
      ns_per_cycle =
          std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(c1 - c0);
    }

    for (std::size_t i = 0; i != config_.num_producers; ++i) {
      producers.emplace_back(&Harness::producer<T, Padding>, this, i, std::ref(ring),
                             std::ref(producer_results[i]), std::cref(collecting), std::cref(done),
                             ns_per_cycle);
    }

    for (std::size_t i = 0; i != config_.num_consumers; ++i) {
      consumers.emplace_back(&Harness::consumer<T, Padding>, this, i, std::ref(ring),
                             std::ref(consumer_results[i]), std::cref(collecting), std::cref(done),
                             ns_per_cycle);
    }

    std::this_thread::sleep_for(config_.warmup_ms);
    const auto measurement_start = std::chrono::steady_clock::now();
    collecting.store(true, std::memory_order_relaxed);

    std::this_thread::sleep_for(config_.duration_ms - config_.warmup_ms);
    done.store(true, std::memory_order_relaxed);

    for (auto& p : producers) {
      p.join();
    }
    for (auto& c : consumers) {
      c.join();
    }
    results.wall_time = std::chrono::steady_clock::now() - measurement_start;

    for (const auto& r : producer_results) {
      results.combine(r);
    }
    for (const auto& r : consumer_results) {
      results.combine(r);
    }

    results.set_latencies(results.push_latencies, results.push_histogram);
    results.set_latencies(results.pop_latencies, results.pop_histogram);
    return results;
  }

  template <typename T, bool Padding>
  void producer(std::size_t id, MpmcRing<T, Padding>& ring, Results& results,
                const std::atomic<bool>& collecting, const std::atomic<bool>& done,
                const double ns_per_cycle) const {
    // pin to CPU core
    if (config_.pinning_on) {
      unsigned num_cores = std::thread::hardware_concurrency();
      int core_id = static_cast<int>(id % num_cores);
      set_thread_affinity_current(core_id);
    }

    results.notes = std::to_string(id);
    uint64_t i = 0;
    uint64_t failures = 1;

    const auto create_item = [](const auto value) {
      if constexpr (std::is_same_v<T, uint64_t>) {
        return T{value};
      } else if constexpr (std::is_same_v<T, std::array<uint64_t, 128>>) {
        std::array<uint64_t, 128> arr{};
        arr.fill(value);
        return arr;
      } else if constexpr (std::is_same_v<T, std::unique_ptr<uint64_t>>) {
        return std::make_unique<uint64_t>(value);
      } else {
        static_assert(std::is_same_v<T, std::unique_ptr<std::array<uint64_t, 128>>>);
        auto arr = std::make_unique<std::array<uint64_t, 128>>();
        arr->fill(value);
        return arr;
      }
    };

    // warmup
    while (!collecting.load(std::memory_order_relaxed)) {
      const auto value = id + config_.num_consumers * i;
      const bool success = config_.blocking ? (ring.push(create_item(value)), true)
                                            : ring.try_push(create_item(value));
      if (success) {
        ++i;
        failures = 1;
      } else {
        backoff(failures);
      }
    }

    while (!done.load(std::memory_order_relaxed)) {
      const auto value = id + config_.num_consumers * i;
      const auto t0 = read_tsc();
      const bool success = config_.blocking ? (ring.push(create_item(value)), true)
                                            : ring.try_push(create_item(value));
      const auto t1 = read_tsc();
      const auto latency = std::chrono::nanoseconds(
          static_cast<int64_t>(static_cast<double>(t1 - t0) * ns_per_cycle));

      if (success) {
        ++i;
        results.push_latencies.min = std::min(results.push_latencies.min, latency);
        results.push_latencies.max = std::max(results.push_latencies.max, latency);
        if ((i % SAMPLE_RATE) == 0) {
          const auto histogram_idx =
              static_cast<std::size_t>(latency.count() / config_.histogram_bucket_width.count());
          if (histogram_idx < config_.histogram_max_buckets) {
            results.push_histogram[histogram_idx] += SAMPLE_RATE;
          } else {
            results.push_overflows += SAMPLE_RATE;
            results.push_latencies.spikes_over_10x_p50 +=
                SAMPLE_RATE; // overflows assumed to be spikes
          }
        }
        ++results.pushes_ok;
        failures = 1;
      } else {
        ++results.try_push_failures;
        backoff(failures);
      }
    }
  }

  template <typename T, bool Padding>
  void consumer(std::size_t id, MpmcRing<T, Padding>& ring, Results& results,
                const std::atomic<bool>& collecting, const std::atomic<bool>& done,
                const double ns_per_cycle) const {
    // pin to CPU core
    if (config_.pinning_on) {
      unsigned num_cores = std::thread::hardware_concurrency();
      int core_id = static_cast<int>((id + config_.num_producers) % num_cores);
      set_thread_affinity_current(core_id);
    }

    results.notes = std::to_string(id);
    uint64_t i = 0;
    uint64_t failures = 1;

    // warmup
    while (!collecting.load(std::memory_order_relaxed)) {
      T out;
      const bool success = config_.blocking ? (ring.pop(out), true) : ring.try_pop(out);
      if (success) {
        ++i;
        failures = 1;
      } else {
        backoff(failures);
      }
    }

    while (!done.load(std::memory_order_relaxed)) {
      T out;
      const auto t0 = read_tsc();
      const bool success = config_.blocking ? (ring.pop(out), true) : ring.try_pop(out);
      const auto t1 = read_tsc();
      const auto latency = std::chrono::nanoseconds(
          static_cast<int64_t>(static_cast<double>(t1 - t0) * ns_per_cycle));

      if (success) {
        ++i;
        results.pop_latencies.min = std::min(results.pop_latencies.min, latency);
        results.pop_latencies.max = std::max(results.pop_latencies.max, latency);
        if ((i % SAMPLE_RATE) == 0) {
          const auto histogram_idx =
              static_cast<std::size_t>(latency.count() / config_.histogram_bucket_width.count());
          if (histogram_idx < config_.histogram_max_buckets) {
            results.pop_histogram[histogram_idx] += SAMPLE_RATE;
          } else {
            results.pop_overflows += SAMPLE_RATE;
            results.pop_latencies.spikes_over_10x_p50 +=
                SAMPLE_RATE; // overflows assumed to be spikes
          }
        }
        ++results.pops_ok;
        failures = 1;
      } else {
        ++results.try_pop_failures;
        backoff(failures);
      }
    }
  }

  inline void backoff(uint64_t& failures) const {
    for (uint64_t i = 0; i < failures; ++i) {
      _mm_pause();
    }
    failures = std::min<uint64_t>(failures * 2, 256);
  }
};

} // namespace mpmc::bench
