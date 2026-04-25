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
    double write_ratio = 0.3;      // fraction of ops that are writes
    double hot_fraction = 0.1;     // fraction of chunks that are "hot"
    double hot_read_weight = 0.8;  // probability a read targets the hot set
    int threads = 4;
    std::string meta_addr = "127.0.0.1:9100";
    std::map<std::string, std::string> node_addrs;
    std::string mode = "adaptive";         // "baseline", "adaptive", or "compare"
    int desired_rf = 3;                    // replication factor sent in every put()
    std::string distribution = "hot_cold"; // "hot_cold", "zipfian", or "uniform"
    double zipf_theta = 0.99;             // Zipf skew parameter (higher = more skewed)
};

class WorkloadGen {
public:
    explicit WorkloadGen(const WorkloadConfig& cfg, HistoryTracer& tracer);
    void run(); // blocks until num_ops complete across all threads

private:
    // Returns {chunk_id, tier_label} where tier_label is "hot" or "cold".
    std::pair<std::string, std::string> pick_chunk();
    void worker_loop(int thread_id, int ops_per_thread);

    WorkloadConfig cfg_;
    HistoryTracer& tracer_;
    NimbusClient client_;
    std::vector<std::string> chunk_ids_;
    int hot_count_;
    int cold_count_;
    double zipf_H_ = 0.0; // Zipf normalisation constant
};

} // namespace nimbus
