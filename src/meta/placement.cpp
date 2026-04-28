#include "placement.h"
#include <algorithm>
#include <random>
#include <spdlog/spdlog.h>

namespace nimbus {

Placement::Placement(const PlacementConfig& cfg) : cfg_(cfg) {}

double Placement::score(const NodeEntry& node,
                         const std::unordered_set<std::string>& existing_set) const {
    double load_score = (1.0 - node.load_fraction) * cfg_.w_load;
    double cap_score  = 0.0;
    if (node.total_bytes > 0) {
        cap_score = (static_cast<double>(node.free_bytes) /
                     static_cast<double>(node.total_bytes)) * cfg_.w_cap;
    }
    double rel_score  = (1.0 - node.failure_rate_7d) * cfg_.w_rel;
    double div_score  = existing_set.count(node.node_id) ? 0.0 : 1.0;
    div_score        *= cfg_.w_div;

    return load_score + cap_score + rel_score + div_score;
}

std::vector<std::string> Placement::select_nodes(
        int n,
        const std::vector<NodeEntry>& alive_nodes,
        const std::unordered_set<std::string>& exclude_set,
        const std::string& /*chunk_id*/) const {

    std::vector<std::string> result;

    // Build candidate list (excluding forbidden nodes)
    std::vector<const NodeEntry*> candidates;
    for (const auto& node : alive_nodes) {
        if (!exclude_set.count(node.node_id))
            candidates.push_back(&node);
    }

    if (cfg_.random_placement) {
        static thread_local std::mt19937 rng{std::random_device{}()};
        std::shuffle(candidates.begin(), candidates.end(), rng);
        for (int i = 0; i < n && i < static_cast<int>(candidates.size()); ++i)
            result.push_back(candidates[i]->node_id);
    } else {
        // Adaptive mode: greedy multi-dimensional scoring.
        std::unordered_set<std::string> chosen;
        for (int i = 0; i < n && !candidates.empty(); ++i) {
            auto best = std::max_element(candidates.begin(), candidates.end(),
                [&](const NodeEntry* a, const NodeEntry* b) {
                    return score(*a, chosen) < score(*b, chosen);
                });
            result.push_back((*best)->node_id);
            chosen.insert((*best)->node_id);
            candidates.erase(best);
        }
    }

    if (static_cast<int>(result.size()) < n) {
        spdlog::warn("Placement: could only select {}/{} nodes", result.size(), n);
    }
    return result;
}

} // namespace nimbus
