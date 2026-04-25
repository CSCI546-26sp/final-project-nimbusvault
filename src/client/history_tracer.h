#pragma once
#include <fstream>
#include <vector>
#include <string>
#include <mutex>

namespace nimbus {

enum class OpType { PUT, GET, DELETE };

struct OpRecord {
    OpType type;
    std::string chunk_id;
    uint64_t start_us;
    uint64_t latency_us;
    bool ok;
    uint64_t bytes;
    std::string tier; // "hot", "cold", "warm", or ""
};

struct SummaryStats {
    size_t   total_ops            = 0;
    size_t   errors               = 0;
    double   throughput_ops_per_s = 0.0;
    uint64_t p50_us               = 0;
    uint64_t p95_us               = 0;
    uint64_t p99_us               = 0;
    double   put_mb_per_s         = 0.0;
    double   get_mb_per_s         = 0.0;
    double   put_ops_per_s        = 0.0;
    double   get_ops_per_s        = 0.0;
};

class HistoryTracer {
public:
    explicit HistoryTracer(const std::string& csv_path);
    ~HistoryTracer();

    void record(OpType type, const std::string& chunk_id,
                uint64_t start_us, uint64_t latency_us,
                bool ok, uint64_t bytes, const std::string& tier = "");

    // Log a promotion or demotion event (written as a special row in the CSV).
    void record_tier_change(const std::string& chunk_id,
                            const std::string& from_tier,
                            const std::string& to_tier,
                            uint64_t ts_us);

    // Compute and return summary statistics (used for the comparison table).
    SummaryStats get_stats() const;

    void print_summary() const;

private:
    std::ofstream file_;
    std::vector<OpRecord> records_;
    mutable std::mutex mu_;
};

} // namespace nimbus
