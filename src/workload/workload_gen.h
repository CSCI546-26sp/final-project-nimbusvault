#pragma once
#include <map>
#include <string>
#include <vector>
#include <random>
#include <thread>
#include "client/history_tracer.h"
#include "client/nimbus_client.h"

namespace nimbus {

struct WorkloadConfig {
    int num_chunks = 100;
    int num_ops = 10000;
    int chunk_size_kb = 4;
    double write_ratio = 0.3; // 30% writes, 70% reads
    double hot_fraction = 0.1; // top 10% of chunks get 80% of reads
    double hot_read_weight = 0.8; // Zipf-like skew
    int threads = 4;
    std::string meta_addr = "127.0.0.1:9100";
    // node_id → address map
    std::map<std::string, std::string> node_addrs;
};

class WorkloadGen {
public:
    explicit WorkloadGen(const WorkloadConfig& cfg, HistoryTracer& tracer);
    void run(); // blocks until num_ops complete across all threads

private:
    std::string pick_chunk(); // hot/cold selection
    void worker_loop(int thread_id, int ops_per_thread);

    WorkloadConfig cfg_;
    HistoryTracer& tracer_;
    NimbusClient client_;
    std::vector<std::string> chunk_ids_; // pre-generated chunk names
    std::mt19937 rng_;
    int hot_count_;
    int cold_count_;
};

} // namespace nimbus
