#include "policy.h"
#include "../common/clock.h"
#include <cmath>
#include <spdlog/spdlog.h>

namespace nimbus {

AdaptivePolicy::AdaptivePolicy(const PolicyConfig& cfg) : cfg_(cfg) {}

void AdaptivePolicy::record_accesses(const std::string& chunk_id,
                                      uint64_t count, uint64_t /*window_ms*/) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& s = state_[chunk_id];
    s.pending_count += count;
    // window_ms is ignored: evaluate() uses wall-clock elapsed time so that a
    // chunk reported in only 1-of-10 heartbeats doesn't appear 10× hotter.
}

PolicyDecision AdaptivePolicy::evaluate(const std::string& chunk_id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& s = state_[chunk_id];
    ChunkTier old_tier = s.tier;

    // Rate = accesses since last evaluate / wall-clock seconds since last evaluate.
    // A chunk that received no heartbeat reports this window gets count=0 → rate=0,
    // which allows hot/warm chunks to decay naturally when traffic stops.
    const uint64_t now = unix_ms();
    const double elapsed_s = (s.last_eval_ms > 0)
        ? std::max(0.1, static_cast<double>(now - s.last_eval_ms) / 1000.0)
        : 2.0;  // first ever evaluate: assume one policy interval

    double window_rate = static_cast<double>(s.pending_count) / elapsed_s;
    s.last_window_rate = window_rate;

    if (s.last_update_ms == 0) {
        s.ewma_rate = window_rate;
    } else {
        double alpha = 1.0 - std::exp(-elapsed_s / cfg_.ewma_half_life_s);
        s.ewma_rate = alpha * window_rate + (1.0 - alpha) * s.ewma_rate;
    }
    s.last_update_ms = now;
    s.last_eval_ms   = now;
    s.pending_count  = 0;

    ChunkTier desired_tier = classify(window_rate);

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
