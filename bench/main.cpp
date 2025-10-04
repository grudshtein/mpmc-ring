#include "harness.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <stdexcept>

namespace mpmc::bench {

Config parse_config(int argc, char* argv[]);
inline void print_usage(const char* prog);
inline bool parse_bool(const std::string& val);
inline std::string to_lower(std::string s);
inline bool is_power_of_two(std::size_t x);

int run(int argc, char* argv[]) {
  try {
    Config config = parse_config(argc, argv);
    const auto t0 = std::chrono::steady_clock::now();
    const auto harness = Harness(config);
    const auto results = harness.run_once();
    const auto t1 = std::chrono::steady_clock::now();
    const auto seconds = std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
    const auto active_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                                    results.config.duration_ms - results.config.warmup_ms)
                                    .count();
    const auto messages_processed = (results.pushes_ok + results.pops_ok) / 2ULL; // total messages
    const double avg_speed =
        static_cast<double>(messages_processed) / static_cast<double>(active_seconds);
    std::cout << "\n[bench] ran in " << seconds << " s\n";
    std::cout << "Messages processed (active phase): " << messages_processed / 1'000'000ULL
              << " million\n";
    std::cout << "Average speed (active phase): " << std::fixed << std::setprecision(1)
              << avg_speed / 1'000'000.0 << " million messages/s\n";
    results.append_csv();
    return 0;
  } catch (const std::invalid_argument& e) {
    std::cerr << "Argument error: " << e.what() << "\n";
    print_usage(argv[0]);
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << "\n";
    return 1;
  }
}

inline bool is_power_of_two(std::size_t x) { return x && !(x & (x - 1)); }

inline std::string to_lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
  return s;
}

inline bool parse_bool(const std::string& val) {
  std::string low = to_lower(val);
  if (low == "on" || low == "true" || low == "1") {
    return true;
  }
  if (low == "off" || low == "false" || low == "0") {
    return false;
  }
  throw std::invalid_argument("Invalid boolean value: " + val);
}

inline void print_usage(const char* prog) {
  std::cerr << "\nUsage: " << prog << " [options]:\n"
            << "  -p, --producers <N>               Number of producers (default: 1)\n"
            << "  -c, --consumers <N>               Number of consumers (default: 1)\n"
            << "  -k, --capacity <POW2>             Ring capacity (default: 256)\n"
            << "  -d, --duration-ms <MS>            Duration in ms (default: 15,000)\n"
            << "  -w, --warmup-ms <MS>              Warmup in ms (default: 2,000)\n"
            << "      --hist-bucket-ns <N>          Histogram bucket width in ns (default: 100)\n"
            << "      --hist-buckets <N>            Max histogram buckets (default: 1024)\n"
            << "      --pinning <on|off>            Thread affinity (default: off)\n"
            << "      --padding <on|off>            Padding toggle (default: off)\n"
            << "      --large-payload <on|off>      Use large payload type (default: off)\n"
            << "      --move-only-payload <on|off>  Use move-only payload type (default: off)\n"
            << "      --csv <PATH>                  CSV output path\n"
            << "      --notes <STRING>              Notes for this run (default: \"\")\n"
            << "  -h, --help                        Show this help message\n";
}

bench::Config parse_config(int argc, char* argv[]) {
  Config config;

  // iterate through args
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto require_value = [&](const std::string& opt) -> std::string {
      if (++i >= argc)
        throw std::invalid_argument("Missing value for " + opt);
      return argv[i];
    };

    if (arg == "--producers" || arg == "-p") {
      config.num_producers = std::stoull(require_value(arg));
    } else if (arg == "--consumers" || arg == "-c") {
      config.num_consumers = std::stoull(require_value(arg));
    } else if (arg == "--capacity" || arg == "-k") {
      config.capacity = std::stoull(require_value(arg));
    } else if (arg == "--duration-ms" || arg == "-d") {
      config.duration_ms = std::chrono::milliseconds(std::stoll(require_value(arg)));
    } else if (arg == "--warmup-ms" || arg == "-w") {
      config.warmup_ms = std::chrono::milliseconds(std::stoll(require_value(arg)));
    } else if (arg == "--hist-bucket-ns") {
      config.histogram_bucket_width = std::chrono::nanoseconds(std::stoll(require_value(arg)));
    } else if (arg == "--hist-buckets") {
      config.histogram_max_buckets = std::stoull(require_value(arg));
    } else if (arg == "--pinning") {
      config.pinning_on = parse_bool(require_value(arg));
    } else if (arg == "--padding") {
      config.padding_on = parse_bool(require_value(arg));
    } else if (arg == "--large-payload") {
      config.large_payload = parse_bool(require_value(arg));
    } else if (arg == "--move-only-payload") {
      config.move_only_payload = parse_bool(require_value(arg));
    } else if (arg == "--csv") {
      config.csv_path = require_value(arg);
    } else if (arg == "--notes") {
      config.notes = require_value(arg);
    } else if (arg == "--help" || arg == "-h") {
      print_usage(argv[0]);
      std::exit(0);
    } else {
      throw std::invalid_argument("Unknown option: " + arg);
    }
  }

  // validation
  if (config.num_producers == 0) {
    throw std::invalid_argument("num_producers must be > 0");
  }
  if (config.num_consumers == 0) {
    throw std::invalid_argument("num_consumers must be > 0");
  }
  if (!is_power_of_two(config.capacity)) {
    throw std::invalid_argument("capacity must be a power of two");
  }
  if (config.duration_ms <= config.warmup_ms) {
    throw std::invalid_argument("total duration must be greater than warmup time");
  }
  if (config.histogram_bucket_width.count() == 0) {
    throw std::invalid_argument("histogram bucket width must be > 0");
  }
  if (config.histogram_max_buckets == 0) {
    throw std::invalid_argument("histogram bucket count must be > 0");
  }

  // echo config
  std::cout << "\nConfiguration:\n"
            << "  producers: " << config.num_producers << "\n"
            << "  consumers: " << config.num_consumers << "\n"
            << "  capacity: " << config.capacity << "\n"
            << "  duration (ms): " << config.duration_ms.count() << "\n"
            << "  warmup (ms): " << config.warmup_ms.count() << "\n"
            << "  pinning: " << (config.pinning_on ? "on" : "off") << "\n"
            << "  padding: " << (config.padding_on ? "on" : "off") << "\n"
            << "  large payload: " << (config.large_payload ? "on" : "off") << "\n"
            << "  move-only payload: " << (config.move_only_payload ? "on" : "off") << "\n"
            << "  csv_path: " << config.csv_path << "\n"
            << "  notes: " << config.notes << "\n";

  return config;
}

} // namespace mpmc::bench

int main(int argc, char* argv[]) { return mpmc::bench::run(argc, argv); }