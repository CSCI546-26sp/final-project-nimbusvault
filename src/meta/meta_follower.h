#pragma once
#include "meta_wal.h"
#include "../common/config.h"
#include <functional>
#include <string>
#include <cstdint>

namespace nimbus {

using CommitCallback = std::function<void(uint64_t log_index)>;

class MetaFollower {
public:
    MetaFollower(MetaWal& wal, const NodeConfig& cfg, CommitCallback on_commit);

    bool handle_append(uint64_t leader_term,
                       uint64_t prev_log_index, uint64_t prev_log_term,
                       uint64_t log_index, uint64_t term,
                       WalEntryType type, const std::string& payload,
                       uint64_t leader_committed_lsn);

    void handle_commit(uint64_t log_index, uint64_t term);

    void install_snapshot(const std::string& snapshot_data, uint64_t snapshot_lsn);

    uint64_t catch_up_from() const;

    uint64_t local_committed() const;
    uint64_t current_term() const { return current_term_; }

private:
    MetaWal&       wal_;
    NodeConfig     cfg_;
    CommitCallback on_commit_;
    uint64_t       current_term_ = 0;
};

} // namespace nimbus
