#include "client/history_tracer.h"
#include <algorithm>
#include <cmath>
#include <iostream>
#include <iomanip>

namespace nimbus {

static const char* OpTypeToString(OpType t) {
    switch (t) {
        case OpType::PUT: return "PUT";
        case OpType::GET: return "GET";
        case OpType::DELETE: return "DELETE";
        default: return "UNKNOWN";
    }
}

HistoryTracer::HistoryTracer(const std::string& csv_path) {
    file_.open(csv_path, std::ios::out);
    if (file_.is_open()) {
        file_ << "timestamp_us,op_type,chunk_id,latency_us,ok,bytes\n";
        file_.flush();
    }
}

HistoryTracer::~HistoryTracer() {
    std::lock_guard<std::mutex> lk(mu_);
    if (file_.is_open()) {
        file_.flush();
        file_.close();
    }
}

void HistoryTracer::record(OpType type, const std::string& chunk_id,
                           uint64_t start_us, uint64_t latency_us,
                           bool ok, uint64_t bytes) {
    std::lock_guard<std::mutex> lk(mu_);
    if (file_.is_open()) {
        file_ << start_us << "," << OpTypeToString(type) << "," << chunk_id
              << "," << latency_us << "," << (ok ? 1 : 0) << "," << bytes << "\n";
        file_.flush();
    }
    records_.push_back(OpRecord{type, chunk_id, start_us, latency_us, ok, bytes});
}

void HistoryTracer::print_summary() const {
    std::vector<OpRecord> copy;
    {
        std::lock_guard<std::mutex> lk(mu_);
        copy = records_;
    }

    size_t total = copy.size();
    size_t errors = 0;
    uint64_t earliest = 0, latest_end = 0;
    uint64_t bytes_put = 0, bytes_get = 0;
    size_t ops_put = 0, ops_get = 0;
    std::vector<uint64_t> latencies;
    latencies.reserve(copy.size());

    for (const auto& r : copy) {
        if (!r.ok) errors++;
        if (earliest == 0 || r.start_us < earliest) earliest = r.start_us;
        uint64_t end_us = r.start_us + r.latency_us;
        if (end_us > latest_end) latest_end = end_us;
        latencies.push_back(r.latency_us);
        if (r.type == OpType::PUT) { ops_put++; bytes_put += r.bytes; }
        if (r.type == OpType::GET) { ops_get++; bytes_get += r.bytes; }
    }

    double duration_s = 1.0;
    if (earliest != 0 && latest_end > earliest) {
        duration_s = static_cast<double>(latest_end - earliest) / 1e6;
        if (duration_s <= 0.0) duration_s = 1.0;
    }

    auto percentile = [&](const std::vector<uint64_t>& v, double p) -> uint64_t {
        if (v.empty()) return 0;
        size_t n = v.size();
        size_t idx = static_cast<size_t>(std::ceil((p/100.0) * n));
        if (idx == 0) idx = 1;
        if (idx > n) idx = n;
        return v[idx-1];
    };

    std::sort(latencies.begin(), latencies.end());
    uint64_t p50 = percentile(latencies, 50.0);
    uint64_t p95 = percentile(latencies, 95.0);
    uint64_t p99 = percentile(latencies, 99.0);

    double ops_per_sec = total / duration_s;
    double put_ops_per_sec = ops_put / duration_s;
    double get_ops_per_sec = ops_get / duration_s;
    double put_mb_per_sec = (static_cast<double>(bytes_put) / (1024.0*1024.0)) / duration_s;
    double get_mb_per_sec = (static_cast<double>(bytes_get) / (1024.0*1024.0)) / duration_s;

    double err_rate = total ? (static_cast<double>(errors) / static_cast<double>(total)) * 100.0 : 0.0;

    std::cout << "----- HistoryTracer Summary -----\n";
    std::cout << "Total ops: " << total << "  Errors: " << errors << " (" << std::fixed << std::setprecision(2) << err_rate << "% )\n";
    std::cout << "Duration (s): " << std::fixed << std::setprecision(3) << duration_s << "\n";
    std::cout << "Throughput: ops/sec=" << std::fixed << std::setprecision(2) << ops_per_sec
              << "  PUT ops/sec=" << put_ops_per_sec << "  GET ops/sec=" << get_ops_per_sec << "\n";
    std::cout << "Bandwidth: PUT MB/s=" << std::fixed << std::setprecision(3) << put_mb_per_sec
              << "  GET MB/s=" << get_mb_per_sec << "\n";
    std::cout << "Latency (us): p50=" << p50 << "  p95=" << p95 << "  p99=" << p99 << "\n";
    std::cout << "---------------------------------\n";
}

} // namespace nimbus
