#include <iostream>
#include <iomanip>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include "workload/workload_gen.h"
#include "client/history_tracer.h"

static std::map<std::string, std::string> parse_nodes(const std::string& s) {
    std::map<std::string, std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        auto pos = item.find('=');
        if (pos == std::string::npos) continue;
        out[item.substr(0, pos)] = item.substr(pos + 1);
    }
    return out;
}

// Run one workload pass and return its stats. Writes a CSV to csv_path.
static nimbus::SummaryStats run_pass(nimbus::WorkloadConfig cfg,
                                     const std::string& csv_path,
                                     const std::string& label) {
    std::cout << "\n--- Running " << label << " ---\n";
    nimbus::HistoryTracer tracer(csv_path);
    nimbus::WorkloadGen gen(cfg, tracer);
    gen.run();
    tracer.print_summary();
    return tracer.get_stats();
}

static void print_comparison(const std::string& label_a, const nimbus::SummaryStats& a,
                              const std::string& label_b, const nimbus::SummaryStats& b) {
    auto err_pct = [](const nimbus::SummaryStats& s) -> double {
        return s.total_ops ? 100.0 * s.errors / s.total_ops : 0.0;
    };

    const int w = 22;
    std::cout << "\n========== Mode Comparison ==========\n";
    std::cout << std::left << std::setw(w) << "Metric"
              << std::setw(14) << label_a
              << std::setw(14) << label_b << "\n";
    std::cout << std::string(w + 28, '-') << "\n";

    auto row = [&](const std::string& name, auto va, auto vb) {
        std::cout << std::left << std::setw(w) << name
                  << std::setw(14) << va
                  << std::setw(14) << vb << "\n";
    };

    row("Total ops",         a.total_ops,                         b.total_ops);
    row("Errors (%)",        std::to_string(a.errors) + " (" +
                                std::to_string((int)err_pct(a)) + "%)",
                             std::to_string(b.errors) + " (" +
                                std::to_string((int)err_pct(b)) + "%)");
    row("Throughput (ops/s)",
        static_cast<int>(a.throughput_ops_per_s),
        static_cast<int>(b.throughput_ops_per_s));
    row("p50 latency (us)",  a.p50_us, b.p50_us);
    row("p95 latency (us)",  a.p95_us, b.p95_us);
    row("p99 latency (us)",  a.p99_us, b.p99_us);
    row("PUT MB/s",
        std::to_string(a.put_mb_per_s).substr(0, 5),
        std::to_string(b.put_mb_per_s).substr(0, 5));
    row("GET MB/s",
        std::to_string(a.get_mb_per_s).substr(0, 5),
        std::to_string(b.get_mb_per_s).substr(0, 5));
    std::cout << "=====================================\n";
}

int main(int argc, char** argv) {
    nimbus::WorkloadConfig cfg;
    std::string nodes_str;
    std::string output = "results.csv";

    for (int i = 1; i < argc; ++i) {
        std::string a(argv[i]);
        auto eq = a.find('=');
        std::string key, val;
        if (eq != std::string::npos) {
            key = a.substr(0, eq);
            val = a.substr(eq + 1);
        } else {
            key = a;
            if (i + 1 < argc) val = argv[++i];
        }

        if      (key == "--meta")         cfg.meta_addr    = val;
        else if (key == "--nodes")        nodes_str        = val;
        else if (key == "--mode")         cfg.mode         = val;
        else if (key == "--chunks")       cfg.num_chunks   = std::stoi(val);
        else if (key == "--ops")          cfg.num_ops      = std::stoi(val);
        else if (key == "--chunk-size")   cfg.chunk_size_kb = std::stoi(val);
        else if (key == "--write-ratio")  cfg.write_ratio  = std::stod(val);
        else if (key == "--hot-fraction") cfg.hot_fraction = std::stod(val);
        else if (key == "--threads")      cfg.threads      = std::stoi(val);
        else if (key == "--desired-rf")   cfg.desired_rf   = std::stoi(val);
        else if (key == "--distribution") cfg.distribution = val;
        else if (key == "--zipf-theta")   cfg.zipf_theta   = std::stod(val);
        else if (key == "--output")       output           = val;
    }

    if (!nodes_str.empty()) cfg.node_addrs = parse_nodes(nodes_str);

    if (cfg.mode == "compare") {
        // Run baseline (fixed rf=3) then adaptive, then print side-by-side.
        nimbus::WorkloadConfig baseline_cfg  = cfg;
        baseline_cfg.mode       = "baseline";
        baseline_cfg.desired_rf = 3;

        nimbus::WorkloadConfig adaptive_cfg  = cfg;
        adaptive_cfg.mode       = "adaptive";

        // Derive output filenames from the base name.
        std::string base = output;
        auto dot = base.rfind('.');
        std::string stem = (dot != std::string::npos) ? base.substr(0, dot) : base;
        std::string ext  = (dot != std::string::npos) ? base.substr(dot)    : ".csv";

        auto stats_b = run_pass(baseline_cfg, stem + "_baseline" + ext, "BASELINE");
        auto stats_a = run_pass(adaptive_cfg, stem + "_adaptive" + ext, "ADAPTIVE");
        print_comparison("BASELINE", stats_b, "ADAPTIVE", stats_a);
    } else {
        std::cout << "Starting workload: mode=" << cfg.mode
                  << ", ops=" << cfg.num_ops
                  << ", chunks=" << cfg.num_chunks
                  << ", rf=" << cfg.desired_rf << "\n";
        nimbus::HistoryTracer tracer(output);
        nimbus::WorkloadGen gen(cfg, tracer);
        gen.run();
        tracer.print_summary();
    }

    return 0;
}
