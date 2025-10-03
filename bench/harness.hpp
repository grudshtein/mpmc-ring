#include "mpmc.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
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
  // Windows implementation (SetThreadAffinityMask)
  DWORD_PTR mask = (1ull << core_id);
  if (SetThreadAffinityMask(GetCurrentThread(), mask) == 0) {
    throw std::runtime_error("SetThreadAffinityMask failed (self)");
  }
#else
  (void)core_id;
#endif
}

struct Config {
  std::size_t num_producers{1};
  std::size_t num_consumers{1};
  std::size_t capacity{65'536};
  std::chrono::milliseconds duration_ms{17'500};
  std::chrono::milliseconds warmup_ms{2'500};
  std::chrono::nanoseconds histogram_bucket_width{100};
  std::size_t histogram_max_buckets{1'024};
  bool pinning_on{true};
  bool padding_on{true};
  bool trivial_payload{true};
  std::string csv_path{"results/raw/results.csv"};
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
  std::vector<uint64_t> push_histogram{}; // counts per bucket
  std::vector<uint64_t> pop_histogram{};  // counts per bucket

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
    if (config_.trivial_payload) {
      return run_once<uint64_t>();
    } else {
      return run_once<std::unique_ptr<uint64_t>>();
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

    for (std::size_t i = 0; i != config_.num_producers; ++i) {
      producers.emplace_back(&Harness::producer<T, Padding>, this, i, std::ref(ring),
                             std::ref(producer_results[i]), std::cref(collecting), std::cref(done));
    }

    for (std::size_t i = 0; i != config_.num_consumers; ++i) {
      consumers.emplace_back(&Harness::consumer<T, Padding>, this, i, std::ref(ring),
                             std::ref(consumer_results[i]), std::cref(collecting), std::cref(done));
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
                const std::atomic<bool>& collecting, const std::atomic<bool>& done) const {
    // pin to CPU core
    if (config_.pinning_on) {
      unsigned num_cores = std::thread::hardware_concurrency();
      int core_id = static_cast<int>(id % num_cores);
      set_thread_affinity_current(core_id);
    }

    results.notes = std::to_string(id);
    uint64_t i = 0;

    // warmup
    while (!collecting.load(std::memory_order_relaxed)) {
      const auto value = id + config_.num_consumers * i;
      const bool success = [&ring, &value]() {
        if constexpr (std::is_same_v<T, uint64_t>) {
          return ring.try_push(value);
        } else {
          static_assert(std::is_same_v<T, std::unique_ptr<uint64_t>>);
          return ring.try_push(std::make_unique<uint64_t>(value));
        }
      }();
      if (success) {
        ++i;
      }
    }

    while (!done.load(std::memory_order_relaxed)) {
      const auto value = id + config_.num_consumers * i;
      const auto t0 = std::chrono::steady_clock::now();
      const bool success = [&ring, &value]() {
        if constexpr (std::is_same_v<T, uint64_t>) {
          return ring.try_push(value);
        } else {
          static_assert(std::is_same_v<T, std::unique_ptr<uint64_t>>);
          return ring.try_push(std::make_unique<uint64_t>(value));
        }
      }();
      const auto latency =
          duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0);

      if (success) {
        ++i;
        results.push_latencies.min = std::min(results.push_latencies.min, latency);
        results.push_latencies.max = std::max(results.push_latencies.max, latency);
        const auto histogram_idx = std::min(
            static_cast<std::size_t>(latency.count() / config_.histogram_bucket_width.count()),
            config_.histogram_max_buckets - 1);
        ++results.push_histogram[histogram_idx];
        ++results.pushes_ok;
      } else {
        ++results.try_push_failures;
      }
    }
  }

  template <typename T, bool Padding>
  void consumer(std::size_t id, MpmcRing<T, Padding>& ring, Results& results,
                const std::atomic<bool>& collecting, const std::atomic<bool>& done) const {
    // pin to CPU core
    if (config_.pinning_on) {
      unsigned num_cores = std::thread::hardware_concurrency();
      int core_id = static_cast<int>((id + config_.num_producers) % num_cores);
      set_thread_affinity_current(core_id);
    }

    results.notes = std::to_string(id);

    // warmup
    while (!collecting.load(std::memory_order_relaxed)) {
      T out;
      (void)ring.try_pop(out);
    }

    while (!done.load(std::memory_order_relaxed)) {
      T out;
      const auto t0 = std::chrono::steady_clock::now();
      const auto success = ring.try_pop(out);
      const auto latency =
          duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0);

      if (success) {
        results.pop_latencies.min = std::min(results.pop_latencies.min, latency);
        results.pop_latencies.max = std::max(results.pop_latencies.max, latency);
        const auto histogram_idx = std::min(
            static_cast<std::size_t>(latency.count() / config_.histogram_bucket_width.count()),
            config_.histogram_max_buckets - 1);
        ++results.pop_histogram[histogram_idx];
        ++results.pops_ok;
      } else {
        ++results.try_pop_failures;
      }
    }
  }
};

} // namespace mpmc::bench
