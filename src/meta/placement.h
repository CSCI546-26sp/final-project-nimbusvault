#pragma once
#include "meta_coordinator.h"
#include "../common/config.h"
#include <vector>
#include <string>
#include <unordered_set>

namespace nimbus {

class Placement {
public:
    explicit Placement(const PlacementConfig& cfg);

    std::vector<std::string> select_nodes(
        int n,
        const std::vector<NodeEntry>& alive_nodes,
        const std::unordered_set<std::string>& exclude_set,
        const std::string& chunk_id = "") const;

private:
    double score(const NodeEntry& node,
                 const std::unordered_set<std::string>& existing_set) const;

    PlacementConfig cfg_;
};

} // namespace nimbus
