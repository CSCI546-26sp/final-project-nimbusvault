#pragma once
#include "../common/config.h"
#include <string>
#include <unordered_map>
#include <mutex>
#include <cstdint>

namespace nimbus {

enum class ChunkTier { COLD, WARM, HOT };

struct ChunkAccessState {
    double   ewma_rate    = 0.0;   // exponentially weighted moving average req/s
    uint64_t last_update_ms = 0;
    int      windows_above  = 0;   // consecutive windows above threshold
    int      windows_below  = 0;   // consecutive windows below threshold
    ChunkTier tier          = ChunkTier::COLD;
};

struct PolicyDecision {
    std::string chunk_id;
    int         new_rf;
    ChunkTier   new_tier;
    bool        changed;
};

class AdaptivePolicy {
public:
    explicit AdaptivePolicy(const PolicyConfig& cfg);

    // Record accesses for a chunk. Call every stats_interval from heartbeat.
    void record_accesses(const std::string& chunk_id,
                         uint64_t count, uint64_t window_ms);

    // Evaluate tier and desired rf for a chunk. Returns decision.
    PolicyDecision evaluate(const std::string& chunk_id);

    // Get current EWMA rate for a chunk (for metrics).
    double get_rate(const std::string& chunk_id) const;

private:
    ChunkTier classify(double rate) const;
    int rf_for_tier(ChunkTier t) const;

    PolicyConfig cfg_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, ChunkAccessState> state_;
};

} // namespace nimbus
