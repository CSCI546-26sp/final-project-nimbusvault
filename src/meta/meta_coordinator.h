#pragma once
#include "../common/config.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <cstdint>

namespace nimbus {

enum class ChunkConfigState { STABLE, TRANSITIONING };

struct ChunkEntry {
    std::string              chunk_id;
    uint64_t                 version           = 0;
    std::vector<std::string> replica_set;
    int                      replication_factor = 3;
    ChunkConfigState         config_state       = ChunkConfigState::STABLE;
    std::vector<std::string> old_replica_set;
    uint64_t                 size_bytes         = 0;
};

struct NodeEntry {
    std::string node_id;
    std::string address;
    float       load_fraction  = 0.0f;
    uint64_t    free_bytes     = 0;
    uint64_t    total_bytes    = 0;
    float       failure_rate_7d = 0.0f;
    uint64_t    last_seen_ms   = 0;
    bool        alive          = true;
};

class MetaCoordinator {
public:
    explicit MetaCoordinator(const NodeConfig& cfg);

    void apply_put_chunk(const std::string& chunk_id,
                         uint64_t size_bytes, int desired_rf,
                         const std::vector<std::string>& replica_set,
                         uint64_t version);

    void apply_delete_chunk(const std::string& chunk_id);

    void apply_reconfig_start(const std::string& chunk_id,
                              const std::vector<std::string>& old_set,
                              const std::vector<std::string>& new_set);

    void apply_reconfig_commit(const std::string& chunk_id,
                               const std::vector<std::string>& new_set);

    void apply_heartbeat(const std::string& node_id,
                         const std::string& address,
                         float load, uint64_t free_bytes,
                         uint64_t total_bytes, float fail_rate);

    const ChunkEntry* get_chunk(const std::string& chunk_id) const;
    std::vector<ChunkEntry> get_chunks() const;
    std::vector<NodeEntry> get_nodes() const;
    std::vector<NodeEntry> get_alive_nodes() const;

    void restore_snapshot(const std::vector<NodeEntry>& nodes,
                          const std::vector<ChunkEntry>& chunks);

    void evict_stale_nodes(uint64_t now_ms, uint64_t timeout_ms = 3000);

    uint64_t next_version(const std::string& chunk_id);

private:
    NodeConfig cfg_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, ChunkEntry> chunk_table_;
    std::unordered_map<std::string, NodeEntry>  node_table_;
    uint64_t global_version_ = 0;
};

} // namespace nimbus
