#include "workload/workload_gen.h"
#include <chrono>
#include <thread>
#include <random>
#include <iostream>

namespace nimbus {

WorkloadGen::WorkloadGen(const WorkloadConfig& cfg, HistoryTracer& tracer)
    : cfg_(cfg), tracer_(tracer), client_(cfg.meta_addr, cfg.node_addrs) {
    std::random_device rd;
    rng_.seed(rd());

    if (cfg_.hot_fraction <= 0.0) cfg_.hot_fraction = 0.0;
    if (cfg_.hot_fraction > 1.0) cfg_.hot_fraction = 1.0;

    hot_count_ = static_cast<int>(cfg_.num_chunks * cfg_.hot_fraction);
    if (hot_count_ < 1) hot_count_ = 1;
    cold_count_ = cfg_.num_chunks - hot_count_;
    if (cold_count_ < 1) cold_count_ = 1;

    chunk_ids_.reserve(cfg_.num_chunks);
    for (int i = 0; i < cfg_.num_chunks; ++i) {
        chunk_ids_.push_back("chunk_" + std::to_string(i));
    }
}

std::string WorkloadGen::pick_chunk() {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    double r = dist(rng_);
    if (r < cfg_.hot_read_weight) {
        std::uniform_int_distribution<int> d(0, hot_count_ - 1);
        return chunk_ids_[d(rng_)];
    } else {
        std::uniform_int_distribution<int> d(0, cold_count_ - 1);
        return chunk_ids_[hot_count_ + d(rng_)];
    }
}

void WorkloadGen::worker_loop(int thread_id, int ops_per_thread) {
    std::uniform_real_distribution<double> opdist(0.0, 1.0);

    std::string payload(cfg_.chunk_size_kb * 1024, 'x');

    for (int i = 0; i < ops_per_thread; ++i) {
        std::string chunk = pick_chunk();

        auto now = std::chrono::system_clock::now();
        uint64_t start_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());

        bool do_write = (opdist(rng_) < cfg_.write_ratio);
        if (do_write) {
            auto t0 = std::chrono::high_resolution_clock::now();
            PutResult pres = client_.put(chunk, payload);
            auto t1 = std::chrono::high_resolution_clock::now();
            uint64_t latency_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
            tracer_.record(OpType::PUT, chunk, start_us, latency_us, pres.ok, payload.size());
        } else {
            auto t0 = std::chrono::high_resolution_clock::now();
            GetResult gres = client_.get(chunk);
            auto t1 = std::chrono::high_resolution_clock::now();
            uint64_t latency_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
            tracer_.record(OpType::GET, chunk, start_us, latency_us, gres.ok, gres.data.size());
        }
    }
}

void WorkloadGen::run() {
    // Pre-create all chunks
    std::string payload(cfg_.chunk_size_kb * 1024, 'x');
    for (const auto& cid : chunk_ids_) {
        (void)client_.put(cid, payload);
    }

    int threads = std::max(1, cfg_.threads);
    int base = cfg_.num_ops / threads;
    int rem = cfg_.num_ops % threads;

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        int ops = base + (t < rem ? 1 : 0);
        workers.emplace_back([this, t, ops]() { worker_loop(t, ops); });
    }

    for (auto& th : workers) {
        if (th.joinable()) th.join();
    }
}

} // namespace nimbus
