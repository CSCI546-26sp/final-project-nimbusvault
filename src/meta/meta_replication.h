#pragma once
#include "meta_wal.h"
#include "../common/config.h"
#include <grpcpp/grpcpp.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <functional>

namespace nimbus {

enum class FollowerStatus { HEALTHY, LAGGING, DEAD };

struct FollowerState {
    std::string    address;
    uint64_t       match_index    = 0; 
    uint64_t       next_index     = 1;
    FollowerStatus status         = FollowerStatus::HEALTHY;
    uint64_t       last_ack_ms    = 0;
    int            missed_pings   = 0;
};

enum class WriteResult { OK, UNAVAILABLE, TIMEOUT };

using CommitCallback = std::function<void(uint64_t log_index)>;

class MetaReplication {
public:
    MetaReplication(MetaWal& wal,
                    const NodeConfig& cfg,
                    CommitCallback on_commit);
    ~MetaReplication();

    void add_follower(const std::string& address);

    WriteResult write(WalEntryType type, const std::string& payload);

    void record_ack(const std::string& follower_addr, uint64_t log_index);

    void send_heartbeats();

    uint64_t current_term() const { return term_.load(); }
    bool is_leader() const { return true; }  

private:
    void replicate_to_follower(const std::string& addr, uint64_t log_index,
                               WalEntryType type, uint64_t term,
                               const std::string& payload);

    MetaWal&          wal_;
    NodeConfig        cfg_;
    CommitCallback    on_commit_;

    std::atomic<uint64_t>   term_{1};
    std::mutex              mu_;
    std::unordered_map<std::string, FollowerState> followers_;
    std::unordered_map<uint64_t, int> ack_counts_;
};

} // namespace nimbus
