#include "meta_coordinator.h"
#include "../common/clock.h"
#include <spdlog/spdlog.h>
#include <algorithm>

namespace nimbus {

MetaCoordinator::MetaCoordinator(const NodeConfig& cfg) : cfg_(cfg) {}

void MetaCoordinator::apply_put_chunk(const std::string& chunk_id,
                                      uint64_t size_bytes, int desired_rf,
                                      const std::vector<std::string>& replica_set,
                                      uint64_t version) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& e = chunk_table_[chunk_id];
    e.chunk_id          = chunk_id;
    e.version           = version;
    e.size_bytes        = size_bytes;
    e.replication_factor = desired_rf;
    e.replica_set       = replica_set;
    e.config_state      = ChunkConfigState::STABLE;
    e.old_replica_set.clear();
    spdlog::debug("coordinator: PUT_CHUNK {} v={}", chunk_id, version);
}

void MetaCoordinator::apply_delete_chunk(const std::string& chunk_id) {
    std::lock_guard<std::mutex> lk(mu_);
    chunk_table_.erase(chunk_id);
    spdlog::debug("coordinator: DELETE_CHUNK {}", chunk_id);
}

void MetaCoordinator::apply_reconfig_start(const std::string& chunk_id,
                                            const std::vector<std::string>& old_set,
                                            const std::vector<std::string>& new_set) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = chunk_table_.find(chunk_id);
    if (it == chunk_table_.end()) return;
    it->second.config_state    = ChunkConfigState::TRANSITIONING;
    it->second.old_replica_set = old_set;
    it->second.replica_set     = new_set;
    spdlog::info("coordinator: RECONFIG_START {} old_sz={} new_sz={}",
                 chunk_id, old_set.size(), new_set.size());
}

void MetaCoordinator::apply_reconfig_commit(const std::string& chunk_id,
                                             const std::vector<std::string>& new_set) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = chunk_table_.find(chunk_id);
    if (it == chunk_table_.end()) return;
    it->second.config_state = ChunkConfigState::STABLE;
    it->second.replica_set  = new_set;
    it->second.old_replica_set.clear();
    spdlog::info("coordinator: RECONFIG_COMMIT {} rf={}", chunk_id, new_set.size());
}

void MetaCoordinator::apply_heartbeat(const std::string& node_id,
                                       const std::string& address,
                                       float load, uint64_t free_bytes,
                                       uint64_t total_bytes, float fail_rate) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& n           = node_table_[node_id];
    n.node_id         = node_id;
    n.address         = address;
    n.load_fraction   = load;
    n.free_bytes      = free_bytes;
    n.total_bytes     = total_bytes;
    n.failure_rate_7d = fail_rate;
    n.last_seen_ms    = unix_ms();
    n.alive           = true;
}

const ChunkEntry* MetaCoordinator::get_chunk(const std::string& chunk_id) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = chunk_table_.find(chunk_id);
    return (it != chunk_table_.end()) ? &it->second : nullptr;
}

std::vector<NodeEntry> MetaCoordinator::get_nodes() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<NodeEntry> out;
    out.reserve(node_table_.size());
    for (auto& [_, n] : node_table_) out.push_back(n);
    return out;
}

std::vector<NodeEntry> MetaCoordinator::get_alive_nodes() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<NodeEntry> out;
    for (auto& [_, n] : node_table_) {
        if (n.alive) out.push_back(n);
    }
    return out;
}

void MetaCoordinator::evict_stale_nodes(uint64_t now_ms_val, uint64_t timeout_ms) {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& [id, n] : node_table_) {
        if (n.alive && (now_ms_val - n.last_seen_ms) > timeout_ms) {
            n.alive = false;
            spdlog::warn("coordinator: node {} declared dead (no heartbeat {}ms)",
                         id, timeout_ms);
        }
    }
}

uint64_t MetaCoordinator::next_version(const std::string& chunk_id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = chunk_table_.find(chunk_id);
    uint64_t base = (it != chunk_table_.end()) ? it->second.version : 0;
    return base + 1;
}

} // namespace nimbus
