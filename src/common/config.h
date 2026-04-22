#pragma once
#include <string>
#include <vector>
#include <cstdint>

namespace nimbus {

struct NodeConfig {
    std::string node_id;
    std::string address;
    std::string role;      
    std::string data_dir;
    std::vector<std::string> peers;
    uint64_t capacity_bytes = 0;
    std::string mode = "adaptive"; 

    uint32_t heartbeat_interval_ms  = 200;
    uint32_t follower_timeout_ms    = 600;
    uint32_t quorum_timeout_ms      = 2000;

    uint32_t stats_interval_ms = 1000;
};

struct PolicyConfig {
    double cold_threshold  = 0.1;  
    double hot_threshold   = 1.0;
    int    cold_rf         = 2;
    int    warm_rf         = 3;
    int    hot_rf          = 5;
    double ewma_half_life_s = 60.0;
    int    promote_windows  = 2;
    int    demote_windows   = 3;
};

struct PlacementConfig {
    double w_load = 0.3;
    double w_cap  = 0.3;
    double w_rel  = 0.2;
    double w_div  = 0.2;
};

} // namespace nimbus
