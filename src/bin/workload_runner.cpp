#include <iostream>
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
        std::string id = item.substr(0, pos);
        std::string addr = item.substr(pos+1);
        out[id] = addr;
    }
    return out;
}

int main(int argc, char** argv) {
    nimbus::WorkloadConfig cfg;
    std::string nodes_str;
    std::string mode = "adaptive";
    std::string output = "results.csv";

    for (int i = 1; i < argc; ++i) {
        std::string a(argv[i]);
        auto eq = a.find('=');
        std::string key, val;
        if (eq != std::string::npos) {
            key = a.substr(0, eq);
            val = a.substr(eq+1);
        } else {
            key = a;
            if (i+1 < argc) val = argv[++i];
        }

        if (key == "--meta") cfg.meta_addr = val;
        else if (key == "--nodes") nodes_str = val;
        else if (key == "--mode") mode = val;
        else if (key == "--chunks") cfg.num_chunks = std::stoi(val);
        else if (key == "--ops") cfg.num_ops = std::stoi(val);
        else if (key == "--chunk-size") cfg.chunk_size_kb = std::stoi(val);
        else if (key == "--write-ratio") cfg.write_ratio = std::stod(val);
        else if (key == "--hot-fraction") cfg.hot_fraction = std::stod(val);
        else if (key == "--threads") cfg.threads = std::stoi(val);
        else if (key == "--output") output = val;
    }

    if (!nodes_str.empty()) cfg.node_addrs = parse_nodes(nodes_str);

    nimbus::HistoryTracer tracer(output);
    nimbus::WorkloadGen gen(cfg, tracer);

    std::cout << "Starting workload: mode=" << mode << ", " << cfg.num_ops << " ops, " << cfg.num_chunks << " chunks\n";
    gen.run();
    tracer.print_summary();

    return 0;
}
