#include "harness.hpp"

namespace mpmc::bench {

void Results::combine(const Results& other) {
  // throughput
  pushes_ok += other.pushes_ok;
  pops_ok += other.pops_ok;
  try_push_failures += other.try_push_failures;
  try_pop_failures += other.try_pop_failures;

  // spikes
  push_latencies.worst_spike =
      std::max(push_latencies.worst_spike, other.push_latencies.worst_spike);
  pop_latencies.worst_spike = std::max(pop_latencies.worst_spike, other.pop_latencies.worst_spike);

  // histograms
  for (std::size_t i = 0; i < other.push_histogram.size(); ++i) {
    push_histogram[i] += other.push_histogram[i];
  }
  for (std::size_t i = 0; i < other.pop_histogram.size(); ++i) {
    pop_histogram[i] += other.pop_histogram[i];
  }
}

void Results::set_latencies(LatencyStats& latencies, const std::vector<uint64_t>& histogram) {
  const auto total = std::accumulate(histogram.begin(), histogram.end(), uint64_t{0});
  if (total == 0) {
    return;
  }
  const auto bucket_width = config.histogram_bucket_width;

  // min = first non-empty bucket
  const auto min_it =
      std::find_if(histogram.begin(), histogram.end(), [](const auto count) { return count != 0; });
  const auto min_idx = std::distance(histogram.begin(), min_it);
  latencies.min = (min_idx * bucket_width) + bucket_width / 2;

  // max = last non-empty bucket
  const auto max_it = std::find_if(histogram.rbegin(), histogram.rend(),
                                   [](const auto count) { return count != 0; });
  const auto max_idx =
      histogram.size() - 1 - static_cast<std::size_t>(std::distance(histogram.rbegin(), max_it));
  latencies.max = (max_idx * bucket_width) + bucket_width / 2;

  // cumulative percentile counts
  uint64_t rank50 = total * 50 / 100;
  uint64_t rank95 = total * 95 / 100;
  uint64_t rank99 = total * 99 / 100;
  uint64_t rank999 = total * 999 / 1000;

  uint64_t cumulative = 0;
  size_t p50_idx = 0, p95_idx = 0, p99_idx = 0, p999_idx = 0;
  for (size_t i = 0; i < histogram.size(); ++i) {
    cumulative += histogram[i];
    if (p50_idx == 0 && cumulative >= rank50) {
      p50_idx = i;
    }
    if (p95_idx == 0 && cumulative >= rank95) {
      p95_idx = i;
    }
    if (p99_idx == 0 && cumulative >= rank99) {
      p99_idx = i;
    }
    if (p999_idx == 0 && cumulative >= rank999) {
      p999_idx = i;
    }
  }

  latencies.p50 = (p50_idx * bucket_width) + bucket_width / 2;
  latencies.p95 = (p95_idx * bucket_width) + bucket_width / 2;
  latencies.p99 = (p99_idx * bucket_width) + bucket_width / 2;
  latencies.p999 = (p999_idx * bucket_width) + bucket_width / 2;

  long double weighted_sum = 0.0;
  for (size_t i = 0; i < histogram.size(); ++i) {
    weighted_sum += static_cast<long double>(histogram[i]) * ((i + 0.5L) * bucket_width.count());
  }
  latencies.mean = std::chrono::nanoseconds(static_cast<int64_t>(weighted_sum / total));

  // spikes
  const auto spike_threshold = 10 * latencies.p50.count();
  size_t spike_idx = static_cast<size_t>(spike_threshold / bucket_width.count());
  if (spike_idx < histogram.size()) {
    for (size_t i = spike_idx; i < histogram.size(); ++i) {
      latencies.spikes_over_10x_p50 += histogram[i];
    }
  }
}

void Results::append_csv() const {
  namespace fs = std::filesystem;

  const fs::path path = fs::path(config.csv_path);
  if (path.has_parent_path()) {
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec); // best-effort
  }

  const bool need_header = !fs::exists(path) || fs::file_size(path) == 0;

  std::ofstream out(path, std::ios::out | std::ios::app);
  if (!out) {
    // keep benchmark running.
    std::fprintf(stderr, "Failed to open CSV at '%s'\n", path.string().c_str());
    return;
  }

  if (need_header) {
    write_csv_header(out);
  }
  write_csv_row(out);
  out.flush();
}

void Results::write_csv_header(std::ostream& os) {
  os << "capacity"
     << ",producers"
     << ",consumers"
     << ",padding_on"
     << ",pinning_on"
     << ",warmup_ms"
     << ",duration_ms"
     << ",rng_seed"
     << ",wall_time_ns"

     // throughput
     << ",pushes_ok"
     << ",pops_ok"
     << ",try_push_failures"
     << ",try_pop_failures"
     << ",push_ops_per_sec"
     << ",pop_ops_per_sec"

     // push latency
     << ",push_lat_min_ns"
     << ",push_lat_p50_ns"
     << ",push_lat_p95_ns"
     << ",push_lat_p99_ns"
     << ",push_lat_p999_ns"
     << ",push_lat_max_ns"
     << ",push_lat_mean_ns"

     // pop latency
     << ",pop_lat_min_ns"
     << ",pop_lat_p50_ns"
     << ",pop_lat_p95_ns"
     << ",pop_lat_p99_ns"
     << ",pop_lat_p999_ns"
     << ",pop_lat_max_ns"
     << ",pop_lat_mean_ns"

     // spikes
     << ",push_spikes_over_10x_p50"
     << ",push_worst_spike_ns"
     << ",pop_spikes_over_10x_p50"
     << ",pop_worst_spike_ns"

     // histograms
     << ",hist_bucket_ns"
     << ",push_hist_bins" // semicolon-separated counts
     << ",pop_hist_bins"

     // notes
     << ",notes"
     << "\n";
}

void Results::write_csv_row(std::ostream& os) const {
  // serialize histogram bins as semicolon-separated list
  auto serialize_hist = [](const std::vector<uint64_t>& hist) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < hist.size(); ++i) {
      if (i)
        ss << ';';
      ss << hist[i];
    }
    return ss.str();
  };

  const std::string push_hist_str = serialize_hist(push_histogram);
  const std::string pop_hist_str = serialize_hist(pop_histogram);

  // metadata
  os << config.capacity << ',';
  os << config.num_producers << ',';
  os << config.num_consumers << ',';
  os << (config.padding_on ? 1 : 0) << ',';
  os << (config.pinning_on ? 1 : 0) << ',';
  os << config.warmup_ms.count() << ','; // duration types: output as count()
  os << config.duration_ms.count() << ',';
  os << rng_seed << ',';
  os << wall_time.count() << ',';

  // throughput
  os << pushes_ok << ',';
  os << pops_ok << ',';
  os << try_push_failures << ',';
  os << try_pop_failures << ',';
  os << std::setprecision(6) << std::fixed << push_ops_per_sec() << ',';
  os << std::setprecision(6) << std::fixed << pop_ops_per_sec() << ',';

  // push latency
  os << push_latencies.min.count() << ',';
  os << push_latencies.p50.count() << ',';
  os << push_latencies.p95.count() << ',';
  os << push_latencies.p99.count() << ',';
  os << push_latencies.p999.count() << ',';
  os << push_latencies.max.count() << ',';
  os << std::setprecision(3) << std::fixed << push_latencies.mean.count() << ',';

  // pop latency
  os << pop_latencies.min.count() << ',';
  os << pop_latencies.p50.count() << ',';
  os << pop_latencies.p95.count() << ',';
  os << pop_latencies.p99.count() << ',';
  os << pop_latencies.p999.count() << ',';
  os << pop_latencies.max.count() << ',';
  os << std::setprecision(3) << std::fixed << pop_latencies.mean.count() << ',';

  // spikes
  os << push_latencies.spikes_over_10x_p50 << ',';
  os << push_latencies.worst_spike.count() << ',';
  os << pop_latencies.spikes_over_10x_p50 << ',';
  os << pop_latencies.worst_spike.count() << ',';

  // histograms
  os << config.histogram_bucket_width.count() << ',';
  os << escape_csv(push_hist_str) << ',';
  os << escape_csv(pop_hist_str) << ',';

  // notes
  os << escape_csv(notes) << '\n';
}

std::string Results::escape_csv(std::string_view s) {
  bool need_quotes = s.find_first_of(",\"\n\r") != std::string_view::npos;
  if (!need_quotes) {
    return std::string(s);
  }
  std::string out;
  out.reserve(s.size() + 2);
  out.push_back('"');
  for (char c : s) {
    if (c == '"') {
      out.push_back('"'); // escape quote by doubling
    }
    out.push_back(c);
  }
  out.push_back('"');
  return out;
}

} // namespace mpmc::bench