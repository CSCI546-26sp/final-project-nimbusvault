#include "recovery.h"

#include <spdlog/spdlog.h>

namespace nimbus {

static std::vector<std::string> split_nonempty(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == delim) {
            if (!cur.empty()) out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

std::string resolve_replica_address(const MetaCoordinator& coord,
                                    const std::string& entry) {
    const size_t eq = entry.find('=');
    if (eq != std::string::npos) return entry.substr(eq + 1);
    for (const auto& n : coord.get_alive_nodes()) {
        if (n.node_id == entry) return n.address;
    }
    return {};
}

void cleanup_uncommitted_puts(MetaWal& wal,
                              const MetaCoordinator& coord,
                              const ChunkReplicaDeleteFn& delete_fn) {
    const uint64_t committed = wal.committed_index();
    const uint64_t last = wal.last_log_index();

    for (uint64_t idx = committed + 1; idx <= last; ++idx) {
        WalEntry e;
        if (!wal.read_entry(idx, e)) continue;
        if (e.type != WalEntryType::PUT_CHUNK) continue;

        auto parts = split_nonempty(e.payload, '|');
        if (parts.size() < 5) continue;

        const std::string& chunk_id = parts[0];
        auto replicas = split_nonempty(parts[4], ',');
        for (const auto& replica : replicas) {
            delete_fn(resolve_replica_address(coord, replica), chunk_id);
        }

        spdlog::info("cleanup_uncommitted_puts: removed chunk={} from {} replicas (idx={})",
                     chunk_id, replicas.size(), idx);
    }
}

} // namespace nimbus