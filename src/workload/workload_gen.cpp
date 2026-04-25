#include "workload/workload_gen.h"
#include <chrono>
#include <thread>
#include <random>
#include <cmath>
#include <iostream>

namespace nimbus {

WorkloadGen::WorkloadGen(const WorkloadConfig& cfg, HistoryTracer& tracer)
    : cfg_(cfg), tracer_(tracer), client_(cfg.meta_addr, cfg.node_addrs) {
    if (cfg_.hot_fraction < 0.0) cfg_.hot_fraction = 0.0;
    if (cfg_.hot_fraction > 1.0) cfg_.hot_fraction = 1.0;

    hot_count_ = static_cast<int>(cfg_.num_chunks * cfg_.hot_fraction);
    if (hot_count_ < 1) hot_count_ = 1;
    cold_count_ = cfg_.num_chunks - hot_count_;
    if (cold_count_ < 1) cold_count_ = 1;

    // Pre-compute Zipf normalisation constant H_N = sum_{i=1}^{N} 1/i^theta
    int N = cfg_.num_chunks;
    zipf_H_ = 0.0;
    for (int i = 1; i <= N; ++i)
        zipf_H_ += 1.0 / std::pow(static_cast<double>(i), cfg_.zipf_theta);

    chunk_ids_.reserve(cfg_.num_chunks);
    for (int i = 0; i < cfg_.num_chunks; ++i)
        chunk_ids_.push_back("chunk_" + std::to_string(i));
}

// Draw a Zipf-distributed index in [0, num_chunks-1] via inverse CDF.
static int zipf_sample(int N, double theta, double H_N, std::mt19937& rng) {
    std::uniform_real_distribution<double> u(0.0, 1.0);
    double target = u(rng) * H_N;
    double cumulative = 0.0;
    for (int i = 1; i <= N; ++i) {
        cumulative += 1.0 / std::pow(static_cast<double>(i), theta);
        if (cumulative >= target) return i - 1;
    }
    return N - 1;
}

// Thread-safe: each calling thread gets its own mt19937.
std::pair<std::string, std::string> WorkloadGen::pick_chunk() {
    static thread_local std::mt19937 rng{std::random_device{}()};

    if (cfg_.distribution == "zipfian") {
        int idx = zipf_sample(cfg_.num_chunks, cfg_.zipf_theta, zipf_H_, rng);
        // Lower-index chunks (rank 1,2,...) are hottest under Zipf.
        std::string tier = (idx < hot_count_) ? "hot" : "cold";
        return {chunk_ids_[idx], tier};
    }

    if (cfg_.distribution == "uniform") {
        std::uniform_int_distribution<int> d(0, cfg_.num_chunks - 1);
        int idx = d(rng);
        std::string tier = (idx < hot_count_) ? "hot" : "cold";
        return {chunk_ids_[idx], tier};
    }

    // Default: "hot_cold" biased distribution.
    std::uniform_real_distribution<double> bias(0.0, 1.0);
    if (bias(rng) < cfg_.hot_read_weight) {
        std::uniform_int_distribution<int> d(0, hot_count_ - 1);
        return {chunk_ids_[d(rng)], "hot"};
    } else {
        std::uniform_int_distribution<int> d(0, cold_count_ - 1);
        return {chunk_ids_[hot_count_ + d(rng)], "cold"};
    }
}

void WorkloadGen::worker_loop(int /*thread_id*/, int ops_per_thread) {
    static thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_real_distribution<double> opdist(0.0, 1.0);
    std::string payload(static_cast<size_t>(cfg_.chunk_size_kb) * 1024, 'x');

    for (int i = 0; i < ops_per_thread; ++i) {
        auto [chunk, tier] = pick_chunk();

        auto now = std::chrono::system_clock::now();
        uint64_t start_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                now.time_since_epoch()).count());

        bool do_write = (opdist(rng) < cfg_.write_ratio);
        if (do_write) {
            auto t0 = std::chrono::high_resolution_clock::now();
            PutResult pres = client_.put(chunk, payload, cfg_.desired_rf);
            auto t1 = std::chrono::high_resolution_clock::now();
            uint64_t lat = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
            tracer_.record(OpType::PUT, chunk, start_us, lat, pres.ok, payload.size(), tier);
        } else {
            auto t0 = std::chrono::high_resolution_clock::now();
            GetResult gres = client_.get(chunk);
            auto t1 = std::chrono::high_resolution_clock::now();
            uint64_t lat = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
            tracer_.record(OpType::GET, chunk, start_us, lat, gres.ok, gres.data.size(), tier);
        }
    }
}

void WorkloadGen::run() {
    std::string payload(static_cast<size_t>(cfg_.chunk_size_kb) * 1024, 'x');
    for (const auto& cid : chunk_ids_)
        (void)client_.put(cid, payload, cfg_.desired_rf);

    int threads = std::max(1, cfg_.threads);
    int base    = cfg_.num_ops / threads;
    int rem     = cfg_.num_ops % threads;

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        int ops = base + (t < rem ? 1 : 0);
        workers.emplace_back([this, t, ops]() { worker_loop(t, ops); });
    }
    for (auto& th : workers) if (th.joinable()) th.join();
}

} // namespace nimbus
