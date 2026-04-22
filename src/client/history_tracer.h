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
    uint64_t start_us; // unix microseconds
    uint64_t latency_us; // how long the op took
    bool ok;
    uint64_t bytes; // payload size
};

class HistoryTracer {
public:
    explicit HistoryTracer(const std::string& csv_path);
    ~HistoryTracer(); // flush and close

    void record(OpType type, const std::string& chunk_id,
                uint64_t start_us, uint64_t latency_us,
                bool ok, uint64_t bytes);

    // Print summary to stdout: p50/p95/p99 latency, throughput, error rate
    void print_summary() const;

private:
    std::ofstream file_;
    std::vector<OpRecord> records_; // kept in memory for summary stats
    mutable std::mutex mu_;
};

} // namespace nimbus
