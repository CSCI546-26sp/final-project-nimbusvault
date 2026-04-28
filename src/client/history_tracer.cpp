#include "client/history_tracer.h"
#include <algorithm>
#include <cmath>
#include <iostream>
#include <iomanip>

namespace nimbus {

static const char* op_to_str(OpType t) {
    switch (t) {
        case OpType::PUT:    return "PUT";
        case OpType::GET:    return "GET";
        case OpType::DELETE: return "DELETE";
        default:             return "UNKNOWN";
    }
}

HistoryTracer::HistoryTracer(const std::string& csv_path) {
    file_.open(csv_path, std::ios::out);
    if (file_.is_open()) {
        file_ << "timestamp_us,op_type,chunk_id,latency_us,ok,bytes,tier\n";
        file_.flush();
    }
}

HistoryTracer::~HistoryTracer() {
    std::lock_guard<std::mutex> lk(mu_);
    if (file_.is_open()) { file_.flush(); file_.close(); }
}

void HistoryTracer::record(OpType type, const std::string& chunk_id,
                           uint64_t start_us, uint64_t latency_us,
                           bool ok, uint64_t bytes, const std::string& tier) {
    std::lock_guard<std::mutex> lk(mu_);
    if (file_.is_open()) {
        file_ << start_us << "," << op_to_str(type) << "," << chunk_id
              << "," << latency_us << "," << (ok ? 1 : 0)
              << "," << bytes << "," << tier << "\n";
        file_.flush();
    }
    records_.push_back({type, chunk_id, start_us, latency_us, ok, bytes, tier});
}

void HistoryTracer::record_tier_change(const std::string& chunk_id,
                                       const std::string& from_tier,
                                       const std::string& to_tier,
                                       uint64_t ts_us) {
    std::lock_guard<std::mutex> lk(mu_);
    if (file_.is_open()) {
        // Write as a special TIER_CHANGE row; latency and bytes are 0.
        file_ << ts_us << ",TIER_CHANGE," << chunk_id
              << ",0,1,0," << from_tier << "->" << to_tier << "\n";
        file_.flush();
    }
}

SummaryStats HistoryTracer::get_stats() const {
    std::vector<OpRecord> copy;
    {
        std::lock_guard<std::mutex> lk(mu_);
        copy = records_;
    }

    SummaryStats s;
    s.total_ops = copy.size();
    if (s.total_ops == 0) return s;

    uint64_t earliest = 0, latest_end = 0;
    uint64_t bytes_put = 0, bytes_get = 0;
    size_t ops_put = 0, ops_get = 0;
    std::vector<uint64_t> lat;
    lat.reserve(copy.size());

    for (const auto& r : copy) {
        if (!r.ok) s.errors++;
        if (earliest == 0 || r.start_us < earliest) earliest = r.start_us;
        uint64_t end_us = r.start_us + r.latency_us;
        if (end_us > latest_end) latest_end = end_us;
        lat.push_back(r.latency_us);
        if (r.type == OpType::PUT) { ops_put++; bytes_put += r.bytes; }
        if (r.type == OpType::GET) { ops_get++; bytes_get += r.bytes; }
    }

    double dur_s = 1.0;
    if (earliest && latest_end > earliest)
        dur_s = std::max(1.0, static_cast<double>(latest_end - earliest) / 1e6);

    auto pct = [&](double p) -> uint64_t {
        if (lat.empty()) return 0;
        size_t idx = static_cast<size_t>(std::ceil(p / 100.0 * lat.size()));
        if (idx == 0) idx = 1;
        if (idx > lat.size()) idx = lat.size();
        return lat[idx - 1];
    };
    std::sort(lat.begin(), lat.end());
    s.p50_us               = pct(50.0);
    s.p95_us               = pct(95.0);
    s.p99_us               = pct(99.0);
    s.throughput_ops_per_s = s.total_ops / dur_s;
    s.put_ops_per_s        = ops_put / dur_s;
    s.get_ops_per_s        = ops_get / dur_s;
    s.put_mb_per_s         = (static_cast<double>(bytes_put) / (1024.0 * 1024.0)) / dur_s;
    s.get_mb_per_s         = (static_cast<double>(bytes_get) / (1024.0 * 1024.0)) / dur_s;
    return s;
}

void HistoryTracer::print_summary() const {
    SummaryStats s = get_stats();
    double err_rate = s.total_ops
        ? static_cast<double>(s.errors) / static_cast<double>(s.total_ops) * 100.0
        : 0.0;
    std::cout << "----- HistoryTracer Summary -----\n";
    std::cout << "Total ops: " << s.total_ops
              << "  Errors: " << s.errors
              << " (" << std::fixed << std::setprecision(2) << err_rate << "% )\n";
    std::cout << "Throughput: ops/sec=" << std::fixed << std::setprecision(2)
              << s.throughput_ops_per_s
              << "  PUT ops/sec=" << s.put_ops_per_s
              << "  GET ops/sec=" << s.get_ops_per_s << "\n";
    std::cout << "Bandwidth: PUT MB/s=" << std::fixed << std::setprecision(3)
              << s.put_mb_per_s
              << "  GET MB/s=" << s.get_mb_per_s << "\n";
    std::cout << "Latency (us): p50=" << s.p50_us
              << "  p95=" << s.p95_us
              << "  p99=" << s.p99_us << "\n";
    std::cout << "---------------------------------\n";
}

} // namespace nimbus
