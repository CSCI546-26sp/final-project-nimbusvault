#include "policy.h"
#include "../common/clock.h"
#include <cmath>
#include <spdlog/spdlog.h>

namespace nimbus {

AdaptivePolicy::AdaptivePolicy(const PolicyConfig& cfg) : cfg_(cfg) {}

void AdaptivePolicy::record_accesses(const std::string& chunk_id,
                                      uint64_t count, uint64_t window_ms) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& s = state_[chunk_id];

    double instant_rate = (window_ms > 0)
        ? (static_cast<double>(count) / (window_ms / 1000.0))
        : 0.0;
    s.last_window_rate = instant_rate;

    if (s.last_update_ms == 0) {
        s.ewma_rate = instant_rate;
    } else {
        double dt_s = window_ms / 1000.0;
        double alpha = 1.0 - std::exp(-dt_s / cfg_.ewma_half_life_s);
        s.ewma_rate = alpha * instant_rate + (1.0 - alpha) * s.ewma_rate;
    }
    s.last_update_ms = unix_ms();
}

PolicyDecision AdaptivePolicy::evaluate(const std::string& chunk_id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& s = state_[chunk_id];
    ChunkTier old_tier = s.tier;

    // Drive transition hysteresis using the latest window classification.
    // EWMA is still tracked for metrics/observability via get_rate().
    ChunkTier desired_tier = classify(s.last_window_rate);

    bool changed = false;
    if (desired_tier > s.tier) {
        s.windows_above++;
        s.windows_below = 0;
        if (s.windows_above >= cfg_.promote_windows) {
            s.tier         = desired_tier;
            s.windows_above = 0;
            changed = true;
        }
    } else if (desired_tier < s.tier) {
        s.windows_below++;
        s.windows_above = 0;
        if (s.windows_below >= cfg_.demote_windows) {
            s.tier         = desired_tier;
            s.windows_below = 0;
            changed = true;
        }
    } else {
        s.windows_above = 0;
        s.windows_below = 0;
    }

    if (changed) {
        spdlog::info("policy: chunk={} tier-change rf {} -> {} (rate={:.3f})",
                     chunk_id, rf_for_tier(old_tier), rf_for_tier(s.tier), s.ewma_rate);
    }

    return PolicyDecision{chunk_id, rf_for_tier(desired_tier), desired_tier, changed};
}

double AdaptivePolicy::get_rate(const std::string& chunk_id) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = state_.find(chunk_id);
    return (it != state_.end()) ? it->second.ewma_rate : 0.0;
}

ChunkTier AdaptivePolicy::classify(double rate) const {
    if (rate > cfg_.hot_threshold)  return ChunkTier::HOT;
    if (rate >= cfg_.cold_threshold) return ChunkTier::WARM;
    return ChunkTier::COLD;
}

int AdaptivePolicy::rf_for_tier(ChunkTier t) const {
    switch (t) {
        case ChunkTier::HOT:  return cfg_.hot_rf;
        case ChunkTier::WARM: return cfg_.warm_rf;
        case ChunkTier::COLD: return cfg_.cold_rf;
    }
    return cfg_.warm_rf;
}

} // namespace nimbus
